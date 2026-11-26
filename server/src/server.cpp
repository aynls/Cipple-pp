#include "server.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/system/system_error.hpp>

#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "binary/buffer_writer.hpp"
#include "protocol/cluster/history_lost_response.hpp"
#include "protocol/cluster/retry_response.hpp"
#include "protocol/cluster/wrong_shard_response.hpp"
#include "protocol/configuration/delete_request.hpp"
#include "protocol/configuration/get_request.hpp"
#include "protocol/configuration/incremental_update_request.hpp"
#include "protocol/configuration/put_request.hpp"
#include "protocol/heartbeat/heartbeat_response.hpp"
#include "protocol/replication/sync_response.hpp"
#include "protocol/subscription/dispatch_response.hpp"
#include "protocol/subscription/relay_messages.hpp"
#include "protocol/subscription/subscribe_request.hpp"
#include "protocol/subscription/subscribe_response.hpp"
#include "protocol/subscription/unsubscribe_request.hpp"
#include "protocol/subscription/unsubscribe_response.hpp"
#include "raft/configuration_machine.hpp"
#include "raft/raft_node.hpp"
#include "raft/raft_persistence.hpp"
#include "sharding/shard_layout.hpp"
#include "storage/sqlite_storage.hpp"
#include "subscription/subscriptions.hpp"
#include "tcp/session.hpp"

namespace cipple::server
{
  namespace
  {
    namespace asio = boost::asio;
    using message::MessageType;

    constexpr auto kDeliveryCheckInterval{std::chrono::milliseconds{250}};
    // A GET reply can be as large as a frame. Limit results pending I/O after commit.
    constexpr std::size_t kMaxPendingReads{4};

    bool send(tcp::Session& session, const message::Message& message)
    {
      if (session.async_send(message))
      {
        session.close();
        return false;
      }
      return true;
    }

    void send_result(tcp::Session& session, const Uuid& request_id,
                     const std::optional<std::vector<std::uint8_t>>& result)
    {
      if (!result)
      {
        send(session, message::RetryResponse{request_id});
        return;
      }
      if (result->empty() || result->size() > tcp::kDefaultMaxFrameLength)
      {
        throw std::runtime_error{"invalid committed response size"};
      }
      tcp::BufferWriter frame;
      frame.reserve(tcp::kFrameLengthPrefixSize + result->size());
      frame.write_u32(static_cast<std::uint32_t>(result->size()));
      frame.write_bytes(*result);
      if (session.async_send(std::make_shared<const std::vector<std::uint8_t>>(frame.bytes())))
      {
        session.close();
      }
    }

    struct RoutedTopic
    {
      std::string_view application;
      std::string_view key;
    };

    template <typename Request>
    RoutedTopic request_topic(const message::Message& message) noexcept
    {
      const auto& request{static_cast<const Request&>(message)};
      return {request.application_name(), request.key()};
    }

    std::optional<RoutedTopic> routed_topic(const message::Message& message) noexcept
    {
      switch (message.type())
      {
      case MessageType::kPutRequest:
        return request_topic<message::PutRequest>(message);
      case MessageType::kDeleteRequest:
        return request_topic<message::DeleteRequest>(message);
      case MessageType::kIncrementalUpdateRequest:
        return request_topic<message::IncrementalUpdateRequest>(message);
      case MessageType::kGetRequest:
        return request_topic<message::GetRequest>(message);
      case MessageType::kSubscribeRequest:
        return request_topic<message::SubscribeRequest>(message);
      case MessageType::kUnsubscribeRequest:
        return request_topic<message::UnsubscribeRequest>(message);
      default:
        return std::nullopt;
      }
    }

    void configure_shard(const ShardOwnership& ownership, raft::RaftOptions& options)
    {
      if (!ownership.layout || !options.members.empty())
      {
        throw std::invalid_argument{"sharded startup requires a layout as the only source of Raft membership"};
      }
      const auto* shard{ownership.layout->find(ownership.shard_id)};
      if (shard == nullptr)
      {
        throw std::invalid_argument{"shard layout does not contain the owned shard"};
      }
      options.members.reserve(shard->members.size());
      for (const auto& member : shard->members)
      {
        options.members.push_back({member.id, member.raft.address().to_string() + ':' +
          std::to_string(member.raft.port())});
      }
      if (std::ranges::find(options.members, options.node_id, &raft::RaftMember::id) == options.members.end())
      {
        throw std::invalid_argument{"Raft node id is not a member of the owned shard"};
      }
      options.shard_binding = raft::ShardBinding{ownership.shard_id, ownership.layout->identity()};
    }
  }

  class Server::DatabaseLock final
  {
  public:
    explicit DatabaseLock(const std::filesystem::path& database)
    {
      auto path{database};
      path += ".raft.lock";
      _handle = CreateFileW(path.c_str(), GENERIC_READ | GENERIC_WRITE | DELETE, 0, nullptr, OPEN_ALWAYS,
                            FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE, nullptr);
      if (_handle == INVALID_HANDLE_VALUE)
      {
        throw std::system_error{static_cast<int>(GetLastError()), std::system_category(), "lock server database"};
      }
    }

    ~DatabaseLock() noexcept
    {
      CloseHandle(_handle);
    }

    DatabaseLock(const DatabaseLock&) = delete;
    DatabaseLock& operator=(const DatabaseLock&) = delete;

  private:
    HANDLE _handle;
  };

  void RelayRecoveryOptions::validate() const
  {
    if (!bytes_per_second || burst_bytes < tcp::kDefaultMaxFrameLength + tcp::kFrameLengthPrefixSize)
    {
      throw std::invalid_argument{"relay recovery needs a positive rate and a burst that holds a complete maximum frame"};
    }
  }

  std::shared_ptr<Server> Server::start(asio::io_context& context, const asio::ip::tcp::endpoint& endpoint,
                                        const std::filesystem::path& database, raft::RaftOptions options,
                                        std::optional<ShardOwnership> shard, const RelayRecoveryOptions recovery)
  {
    recovery.validate();
    if (shard)
    {
      configure_shard(*shard, options);
    }
    else if (options.shard_binding)
    {
      throw std::invalid_argument{"a persisted shard binding requires shard ownership"};
    }

    const auto path{std::filesystem::weakly_canonical(database)};
    auto server{std::shared_ptr<Server>{new Server{context, endpoint}}};
    server->_database_lock = std::make_unique<DatabaseLock>(path);

    options.database = path;
    options.database += ".raft";
    const bool has_raft_database{raft::has_initialized_database(options)};
    if (has_raft_database && !std::filesystem::exists(path))
    {
      throw std::runtime_error{"application database missing beside its Raft database"};
    }
    server->_shard = std::move(shard);

    const auto notification_pending{std::make_shared<std::atomic_bool>(false)};
    server->_machine = std::make_shared<raft::ConfigurationMachine>(
      path, options.node_id,
      [&context, weak{server->weak_from_this()}, notification_pending]
      {
        if (!notification_pending->exchange(true))
        {
          asio::post(context, [weak, notification_pending]
          {
            notification_pending->store(false);
            if (const auto self{weak.lock()}; self && self->_acceptor.is_open())
            {
              self->_subscriptions->wake();
              self->schedule_replay();
            }
          });
        }
      });
    if (!has_raft_database && server->_machine->last_commit_index() != 0)
    {
      throw std::runtime_error{"Raft database missing beside a previously applied state machine"};
    }
    server->_subscriptions = std::make_unique<Subscriptions>(server->_machine->reader(), recovery);
    server->_raft = std::make_unique<raft::RaftNode>(options, server->_machine);
    server->accept_next();
    server->check_delivery_deadlines();
    return server;
  }

  Server::Server(asio::io_context& context, const asio::ip::tcp::endpoint& endpoint)
    : _context{context}
      , _acceptor{context, endpoint, false}
      , _delivery_timer{context}
      , _recovery_timer{context}
      , _local_endpoint{_acceptor.local_endpoint()}
  {
  }

  Server::~Server() noexcept
  {
    stop();
  }

  const asio::ip::tcp::endpoint& Server::local_endpoint() const noexcept
  {
    return _local_endpoint;
  }

  void Server::stop() noexcept
  {
    boost::system::error_code ignored;
    _acceptor.close(ignored);
    try
    {
      _delivery_timer.cancel();
      _recovery_timer.cancel();
    }
    catch (const boost::system::system_error&)
    {
      // A timer already ready to run also checks the closed acceptor.
    }
    if (_raft)
    {
      _raft->stop();
    }
    if (_subscriptions)
    {
      _subscriptions->stop();
    }
  }

  void Server::accept_next()
  {
    _acceptor.async_accept([weak{weak_from_this()}](const boost::system::error_code error,
                                                    asio::ip::tcp::socket socket)
    {
      if (const auto self{weak.lock()}; self && self->_acceptor.is_open())
      {
        if (error)
        {
          self->stop();
          throw boost::system::system_error{error, "accept client"};
        }
        self->attach(std::move(socket));
        self->accept_next();
      }
    });
  }

  void Server::attach(asio::ip::tcp::socket socket)
  {
    const auto id{++_next_connection_id};
    const auto weak{weak_from_this()};
    boost::system::error_code error;
    const auto endpoint{socket.remote_endpoint(error)};
    auto address{error ? std::string{} : endpoint.address().to_string()};
    auto session{
      tcp::Session::start(std::move(socket),
                          [weak, id](tcp::Session& connection, std::unique_ptr<message::Message> message)
                          {
                            if (const auto self{weak.lock()})
                            {
                              self->on_message(id, connection, *message);
                            }
                          },
                          [weak, id](boost::system::error_code)
                          {
                            if (const auto self{weak.lock()})
                            {
                              self->_subscriptions->detach(id);
                              if (self->_acceptor.is_open())
                              {
                                self->schedule_replay();
                              }
                            }
                          })
    };
    _subscriptions->attach(id, std::move(session), std::move(address));
  }

  std::optional<std::uint32_t> Server::wrong_shard(const message::Message& message) const noexcept
  {
    if (!_shard)
    {
      return std::nullopt;
    }
    const auto topic{routed_topic(message)};
    if (!topic)
    {
      return std::nullopt;
    }
    const auto& target{_shard->layout->route(topic->application, topic->key)};
    if (target.id == _shard->shard_id)
    {
      return std::nullopt;
    }
    return target.id;
  }

  void Server::submit(tcp::Session& session, const message::Message& command)
  {
    const auto id{command.uuid()};
    const bool reading{command.type() == MessageType::kGetRequest};
    if (reading && _pending_reads >= kMaxPendingReads)
    {
      send(session, message::RetryResponse{id});
      return;
    }

    tcp::BufferWriter body;
    command.encode(body);
    const bool accepted{
      _raft->submit(body.bytes(), [context{&_context}, weak{weak_from_this()}, recipient{session.weak_from_this()},
                                  id, reading](auto result)
      {
        asio::post(*context, [weak, recipient, id, reading, result{std::move(result)}]
        {
          if (const auto self{weak.lock()}; self && self->_acceptor.is_open())
          {
            self->_pending_reads -= reading;
            if (const auto connection{recipient.lock()}; connection && connection->is_open())
            {
              try
              {
                send_result(*connection, id, result);
              }
              catch (const std::exception& error)
              {
                std::cerr << "Response failed: " << error.what() << '\n';
                connection->close();
              }
            }
          }
        });
      })
    };
    if (accepted)
    {
      _pending_reads += reading;
    }
    else
    {
      send(session, message::RetryResponse{id});
    }
  }

  void Server::schedule_replay() noexcept
  {
    if (_replay_scheduled || !_subscriptions->has_ready())
    {
      return;
    }
    try
    {
      asio::post(_context, [weak{weak_from_this()}]
      {
        if (const auto self{weak.lock()}; self && self->_acceptor.is_open())
        {
          self->_replay_scheduled = false;
          self->_subscriptions->replay_ready();
          self->schedule_replay();
          self->schedule_recovery();
        }
      });
      _replay_scheduled = true;
    }
    catch (const std::exception& error)
    {
      std::cerr << "Scheduling subscription replay failed: " << error.what() << '\n';
    }
  }

  void Server::schedule_recovery() noexcept
  {
    const auto due{_subscriptions->recovery_due()};
    if (!due || *due <= std::chrono::steady_clock::now() || _recovery_timer.expiry() == *due)
    {
      return;
    }
    try
    {
      _recovery_timer.expires_at(*due);
      _recovery_timer.async_wait([weak{weak_from_this()}](const boost::system::error_code error)
      {
        if (const auto self{weak.lock()}; self && !error && self->_acceptor.is_open())
        {
          self->schedule_replay();
        }
      });
    }
    catch (const boost::system::system_error& error)
    {
      std::cerr << "Scheduling relay recovery failed: " << error.what() << '\n';
    }
  }

  void Server::check_delivery_deadlines()
  {
    _delivery_timer.expires_after(kDeliveryCheckInterval);
    _delivery_timer.async_wait([weak{weak_from_this()}](const boost::system::error_code error)
    {
      if (const auto self{weak.lock()}; self && !error && self->_acceptor.is_open())
      {
        self->_subscriptions->expire();
        self->schedule_replay();
        self->check_delivery_deadlines();
      }
    });
  }

  void Server::on_message(const std::uint64_t connection_id, tcp::Session& session, const message::Message& message)
  {
    try
    {
      if (const auto target_shard{wrong_shard(message)})
      {
        send(session, message::WrongShardResponse{message.uuid(), *target_shard});
        return;
      }
      switch (message.type())
      {
      case MessageType::kPutRequest:
      case MessageType::kDeleteRequest:
      case MessageType::kIncrementalUpdateRequest:
      case MessageType::kGetRequest:
        submit(session, message);
        break;
      case MessageType::kSubscribeRequest:
        {
          const auto& request{static_cast<const message::SubscribeRequest&>(message)};
          const auto floor{_machine->reader().journal_floor(request.application_name(), request.key())};
          if (request.resume_after() && *request.resume_after() < floor)
          {
            send(session, message::HistoryLostResponse{request.uuid(), floor});
            break;
          }
          const ConfigKey key{request.application_name(), request.key()};
          _subscriptions->subscribe(connection_id, key, request.resume_after());
          if (send(session, message::SubscribeResponse{request.uuid(), true}))
          {
            _subscriptions->enable_relay(connection_id, key);
            schedule_replay();
          }
          break;
        }
      case MessageType::kUnsubscribeRequest:
        {
          const auto& request{static_cast<const message::UnsubscribeRequest&>(message)};
          _subscriptions->unsubscribe(connection_id, {request.application_name(), request.key()});
          send(session, message::UnsubscribeResponse{request.uuid(), true});
          schedule_replay();
          break;
        }
      case MessageType::kDispatchResponse:
        {
          const auto& reply{static_cast<const message::DispatchResponse&>(message)};
          _subscriptions->acknowledge(connection_id, reply.uuid(), reply.success());
          schedule_replay();
          break;
        }
      case MessageType::kRelayAdvertise:
        {
          const auto& advertisement{static_cast<const message::RelayAdvertise&>(message)};
          _subscriptions->advertise(connection_id, advertisement.port(), advertisement.slots());
          break;
        }
      case MessageType::kRelayStatus:
        {
          const auto& status{static_cast<const message::RelayStatus&>(message)};
          _subscriptions->relay_status(connection_id, status.uuid(), status.ready());
          schedule_replay();
          break;
        }
      case MessageType::kSyncRequest:
        send(session, message::SyncResponse{message.uuid(), message::Result::kRejected});
        break;
      case MessageType::kHeartbeatRequest:
        send(session, message::HeartbeatResponse{message.uuid(), true});
        break;
      default:
        session.close();
        break;
      }
    }
    catch (const std::exception& error)
    {
      std::cerr << "Request failed: " << error.what() << '\n';
      session.close();
    }
  }
}
