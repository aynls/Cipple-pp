#include "raft/raft_node.hpp"

#include "raft/raft_persistence.hpp"

#include <libnuraft/asio_service.hxx>
#include <libnuraft/buffer.hxx>
#include <libnuraft/cluster_config.hxx>
#include <libnuraft/context.hxx>
#include <libnuraft/delayed_task_scheduler.hxx>
#include <libnuraft/error_code.hxx>
#include <libnuraft/logger.hxx>
#include <libnuraft/raft_params.hxx>
#include <libnuraft/raft_server.hxx>
#include <libnuraft/rpc_cli_factory.hxx>
#include <libnuraft/rpc_listener.hxx>
#include <libnuraft/snapshot_sync_req.hxx>
#include <libnuraft/state_machine.hxx>

#include <chrono>
#include <condition_variable>
#include <deque>
#include <exception>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>

namespace cipple::raft
{
  namespace
  {
    constexpr std::size_t kMaxPendingCommands{1024};
    constexpr std::size_t kMaxPendingBytes{64u << 20};
    constexpr std::int32_t kClientRequestTimeoutMs{3000};
    using Clock = std::chrono::steady_clock;

    // NuRaft's own warnings can be very noisy during a network partition, but
    // errors and fatal events generally indicate corruption or an unusable
    // local persistent store. Keep those visible without adding a logging
    // dependency to the application.
    class RaftEventLogger final : public nuraft::logger
    {
    public:
      int get_level() override
      {
        return 2;
      }

      void put_details(const int level, const char* source_file, const char* func_name,
                       const size_t line_number, const std::string& log_line) override
      {
        if (level > get_level())
        {
          return;
        }
        const std::lock_guard lock{_mutex};
        std::cerr << "NuRaft [" << level << "] " << source_file << ':' << line_number
                  << " " << func_name << ": " << log_line << '\n';
      }

    private:
      std::mutex _mutex;
    };

    void close_nuraft_part(const std::function<void()>& close) noexcept
    {
      try
      {
        close();
      }
      catch (const std::exception& error)
      {
        std::cerr << "Raft shutdown step failed: " << error.what() << '\n';
      }
      catch (...)
      {
      }
    }
  }

  class RaftNode::Impl
  {
  public:
    Impl(const RaftOptions& options, std::shared_ptr<nuraft::state_machine> state_machine)
      : _state{std::make_shared<SharedState>()}
        , _standalone{options.members.size() == 1}
    {
      if (!state_machine)
      {
        throw std::invalid_argument{"RaftNode needs a state machine"};
      }
      if (options.snapshot_distance < 0)
      {
        throw std::invalid_argument{"Raft snapshot distance must not be negative"};
      }

      try
      {
        _state_manager = std::make_shared<RaftStateManager>(options);
        const auto log_store{std::static_pointer_cast<RaftLogStore>(_state_manager->load_log_store())};
        reconcile_snapshot(*state_machine, *log_store);

        _logger = nuraft::cs_new<RaftEventLogger>();
        nuraft::asio_service::options asio_options;
        asio_options.thread_pool_size_ = 2;
        _asio_service = nuraft::cs_new<nuraft::asio_service>(asio_options, _logger);

        if (!_standalone)
        {
          _listener = _asio_service->create_rpc_listener(_state_manager->self_port(), _logger);
          if (!_listener)
          {
            throw std::runtime_error{"Cannot open Raft RPC listener on port "
                                     + std::to_string(_state_manager->self_port())};
          }
        }

        nuraft::raft_params params;
        params.snapshot_distance_ = options.snapshot_distance;
        params.reserved_log_items_ = 1;
        params.client_req_timeout_ = kClientRequestTimeoutMs;
        params.auto_forwarding_ = false;
        params.auto_adjust_quorum_for_small_cluster_ = false;
        params.parallel_log_appending_ = false;
        params.return_method_ = nuraft::raft_params::blocking;
        params.use_bg_thread_for_urgent_commit_ = true;

        nuraft::raft_server::init_options init;
        init.raft_callback_ = [state_mgr{_state_manager}, log_store, state{_state}](
          const nuraft::cb_func::Type type, nuraft::cb_func::Param* event)
        {
          if (type == nuraft::cb_func::BecomeLeader)
          {
            const std::lock_guard lock{state->mutex};
            state->wakeup.notify_all();
          }
          else if (type == nuraft::cb_func::SaveSnapshot)
          {
            const auto& request{*static_cast<const nuraft::snapshot_sync_req*>(event->ctx)};
            if (request.is_done())
            {
              const auto& snapshot{request.get_snapshot()};
              if (!snapshot.get_last_config())
              {
                return nuraft::cb_func::ReturnCode::ReturnNull;
              }
              state_mgr->check_config(*snapshot.get_last_config());
              log_store->expect_snapshot(snapshot.get_last_log_idx(), snapshot.get_last_log_term());
            }
          }
          return nuraft::cb_func::ReturnCode::Ok;
        };

        nuraft::ptr<nuraft::state_mgr> state_mgr{_state_manager};
        nuraft::ptr<nuraft::state_machine> machine{std::move(state_machine)};
        nuraft::ptr<nuraft::rpc_listener> listener{_listener};
        nuraft::ptr<nuraft::logger> logger{_logger};
        nuraft::ptr<nuraft::rpc_client_factory> client_factory{_asio_service};
        nuraft::ptr<nuraft::delayed_task_scheduler> scheduler{_asio_service};
        // The NuRaft server takes ownership of the context.
        _server = std::unique_ptr<nuraft::raft_server>{new nuraft::raft_server{
          new nuraft::context{state_mgr, machine, listener, logger, client_factory, scheduler, params}, init
        }};
        if (_listener)
        {
          _listener->listen(_server);
        }
        _worker = std::thread{[this] { run(); }};
      }
      catch (...)
      {
        shutdown_nuraft();
        throw;
      }
    }

    ~Impl() noexcept
    {
      stop();
    }

    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;

    bool submit(std::vector<std::uint8_t> command, Completion completion)
    {
      auto request{std::make_shared<Request>()};
      request->charge_bytes = command.size();
      request->expires_at = Clock::now() + std::chrono::milliseconds{kClientRequestTimeoutMs};
      request->command = std::move(command);
      request->completion = std::move(completion);
      request->state = _state;

      {
        const std::lock_guard lock{_state->mutex};
        if (_state->stopping)
        {
          return false;
        }
        if (_state->pending_commands >= kMaxPendingCommands
              || request->charge_bytes > kMaxPendingBytes - _state->pending_bytes)
        {
          return false;
        }
        ++_state->pending_commands;
        _state->pending_bytes += request->charge_bytes;
        try
        {
          _state->queue.push_back(request);
        }
        catch (...)
        {
          --_state->pending_commands;
          _state->pending_bytes -= request->charge_bytes;
          throw;
        }
        request->charge_armed = true;
      }
      _state->wakeup.notify_one();
      return true;
    }

    std::int32_t leader_id() const noexcept
    {
      const std::lock_guard lock{_server_mutex};
      if (!_server)
      {
        return -1;
      }
      return _server->get_leader();
    }

    void stop() noexcept
    {
      const std::lock_guard stop_lock{_stop_mutex};
      if (_stop_started)
      {
        return;
      }
      _stop_started = true;
      {
        const std::lock_guard lock{_state->mutex};
        _state->stopping = true;
      }
      _state->wakeup.notify_all();

      if (_worker.joinable())
      {
        _worker.join();
      }
      shutdown_nuraft();
    }

  private:
    void reconcile_snapshot(nuraft::state_machine& machine, RaftLogStore& store) const
    {
      const auto applied{machine.last_commit_index()};
      const auto snapshot{machine.last_snapshot()};
      if (snapshot)
      {
        const auto index{snapshot->get_last_log_idx()};
        const auto& config{snapshot->get_last_config()};
        if (index > applied || index < store.start_index() - 1 || !config)
        {
          throw std::runtime_error{"Raft snapshot does not match the persisted state machine and log"};
        }
        _state_manager->check_config(*config);
        // Complete a crash-interrupted install before NuRaft starts elections.
        store.expect_snapshot(index, snapshot->get_last_log_term());
        store.compact(index);
        if (config->get_log_idx() > _state_manager->load_config()->get_log_idx())
        {
          _state_manager->save_config(*config);
        }
      }
      else if (store.start_index() > 1)
      {
        throw std::runtime_error{"Raft log was compacted without an available state machine snapshot"};
      }
      if (applied < store.start_index() - 1 || applied >= store.next_slot())
      {
        throw std::runtime_error{"Raft state machine index is outside the persisted log and snapshot"};
      }
    }

    struct SharedState;

    struct Request
    {
      ~Request() noexcept
      {
        if (!charge_armed)
        {
          return;
        }
        const std::lock_guard lock{state->mutex};
        --state->pending_commands;
        state->pending_bytes -= charge_bytes;
      }

      std::vector<std::uint8_t> command;
      Completion completion;
      std::shared_ptr<SharedState> state;
      Clock::time_point expires_at;
      std::size_t charge_bytes{0};
      bool charge_armed{false};
    };

    struct SharedState
    {
      std::mutex mutex;
      std::condition_variable wakeup;
      std::deque<std::shared_ptr<Request>> queue;
      std::size_t pending_commands{0};
      std::size_t pending_bytes{0};
      bool stopping{false};
    };

    void run() noexcept
    {
      for (;;)
      {
        std::shared_ptr<Request> request;
        {
          std::unique_lock lock{_state->mutex};
          _state->wakeup.wait(lock, [this] { return _state->stopping || !_state->queue.empty(); });
          if (_state->queue.empty())
          {
            return;
          }
          request = std::move(_state->queue.front());
          _state->queue.pop_front();
        }

        bool stopping{false};
        {
          const std::lock_guard lock{_state->mutex};
          stopping = _state->stopping;
        }
        if (stopping || Clock::now() >= request->expires_at)
        {
          complete(*request, std::nullopt);
          continue;
        }
        process(*request);
      }
    }

    void process(Request& request) noexcept
    {
      if (_standalone)
      {
        // Allow the first single-node election to finish without closing the
        // client connection and dropping subscriptions on that connection.
        std::unique_lock lock{_state->mutex};
        _state->wakeup.wait_until(lock, request.expires_at, [this]
        {
          return _state->stopping || _server->is_leader();
        });
      }
      std::optional<std::vector<std::uint8_t>> response;
      if (!_server || !_server->is_leader())
      {
        complete(request, std::nullopt);
        return;
      }

      try
      {
        auto command_buffer{nuraft::buffer::alloc(request.command.size())};
        if (!request.command.empty())
        {
          command_buffer->put_raw(request.command.data(), request.command.size());
        }
        command_buffer->pos(0);
        const std::vector<nuraft::ptr<nuraft::buffer>> logs{command_buffer};
        auto result{_server->append_entries(logs)};
        if (result && result->get_accepted() && result->get_result_code() == nuraft::cmd_result_code::OK)
        {
          auto output{result->get()};
          std::vector<std::uint8_t> bytes;
          if (output && output->size() != 0)
          {
            const auto* first{output->data_begin()};
            bytes.assign(first, first + output->size());
          }
          response = std::move(bytes);
        }
      }
      catch (const std::exception& error)
      {
        std::cerr << "Raft submit failed: " << error.what() << '\n';
      }
      catch (...)
      {
      }
      complete(request, std::move(response));
    }

    void complete(Request& request, std::optional<std::vector<std::uint8_t>> response) noexcept
    {
      if (!request.completion)
      {
        return;
      }
      try
      {
        request.completion(std::move(response));
      }
      catch (const std::exception& error)
      {
        std::cerr << "Raft completion callback failed: " << error.what() << '\n';
      }
      catch (...)
      {
      }
    }

    void shutdown_nuraft() noexcept
    {
      if (_listener)
      {
        close_nuraft_part([this] { _listener->stop(); });
      }
      if (_server)
      {
        close_nuraft_part([this] { _server->shutdown(); });
      }
      if (_listener)
      {
        close_nuraft_part([this] { _listener->shutdown(); });
      }
      if (_asio_service)
      {
        close_nuraft_part([this] { _asio_service->stop(); });
      }
      {
        const std::lock_guard lock{_server_mutex};
        _server.reset();
      }
      _listener.reset();
      _asio_service.reset();
      _logger.reset();
      _state_manager.reset();
    }

    std::shared_ptr<SharedState> _state;
    const bool _standalone;
    std::shared_ptr<RaftStateManager> _state_manager;
    nuraft::ptr<nuraft::logger> _logger;
    nuraft::ptr<nuraft::asio_service> _asio_service;
    nuraft::ptr<nuraft::rpc_listener> _listener;
    mutable std::mutex _server_mutex;
    nuraft::ptr<nuraft::raft_server> _server;
    std::thread _worker;
    std::mutex _stop_mutex;
    bool _stop_started{false};
  };

  RaftNode::RaftNode(const RaftOptions& options, std::shared_ptr<nuraft::state_machine> state_machine)
    : _impl{std::make_unique<Impl>(options, std::move(state_machine))}
  {
  }

  RaftNode::~RaftNode() noexcept = default;

  bool RaftNode::submit(std::vector<std::uint8_t> command, Completion completion)
  {
    return _impl->submit(std::move(command), std::move(completion));
  }

  std::int32_t RaftNode::leader_id() const noexcept
  {
    return _impl->leader_id();
  }

  void RaftNode::stop() noexcept
  {
    _impl->stop();
  }
}
