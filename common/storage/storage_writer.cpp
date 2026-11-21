#include "storage/storage_writer.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>

#include <condition_variable>
#include <deque>
#include <exception>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <utility>

#include "storage/storage.hpp"

namespace cipple::storage
{
  struct StorageWriter::SharedState
  {
    std::mutex mutex;
    std::condition_variable wakeup;
    std::deque<std::function<void()>> queue;
    std::size_t request_tasks = 0;
    std::size_t request_bytes = 0;
    std::size_t ack_tasks = 0;
    bool stopping = false;
  };

  // Releases one lane charge when the last reference dies: at task end for
  // an empty done, or when the posted done is consumed or destroyed on
  // `back`. Arm only after the counters were bumped under the state lock;
  // an unarmed charge frees nothing on rejection or unwind.
  class StorageWriter::Charge
  {
  public:
    Charge(std::shared_ptr<SharedState> state, const std::size_t bytes, const bool ack) noexcept
      : _state{std::move(state)}
        , _bytes{bytes}
        , _ack{ack}
    {
    }

    ~Charge() noexcept
    {
      if (_armed)
      {
        const std::lock_guard lock{_state->mutex};
        release();
      }
    }

    Charge(const Charge&) = delete;
    Charge& operator=(const Charge&) = delete;

    // Callers hold _state->mutex.
    void arm() noexcept
    {
      _armed = true;
    }

    void disarm() noexcept
    {
      _armed = false;
    }

    void release() noexcept
    {
      if (_ack)
      {
        --_state->ack_tasks;
      }
      else
      {
        --_state->request_tasks;
        _state->request_bytes -= _bytes;
      }
    }

  private:
    std::shared_ptr<SharedState> _state;
    std::size_t _bytes;
    bool _ack;
    bool _armed = false;
  };

  StorageWriter::StorageWriter(std::unique_ptr<Storage> writer, std::unique_ptr<Storage> reader)
    : StorageWriter{std::move(writer), std::move(reader), Limits{}}
  {
  }

  StorageWriter::StorageWriter(std::unique_ptr<Storage> writer, std::unique_ptr<Storage> reader,
                               const Limits limits)
    : _limits{limits}
      , _writer{std::move(writer)}
      , _reader{std::move(reader)}
      , _state{std::make_shared<SharedState>()}
  {
    if (!_writer || !_reader)
    {
      throw std::invalid_argument{"storage writer needs distinct write and read connections"};
    }
    _thread = std::thread{[this] { run(); }};
  }

  StorageWriter::~StorageWriter() noexcept
  {
    stop();
  }

  Storage& StorageWriter::reader() const noexcept
  {
    return *_reader;
  }

  bool StorageWriter::submit(boost::asio::io_context& back, std::function<void(Storage &)> task,
                             std::function<void()> done, const std::size_t charge_bytes)
  {
    return enqueue(back, std::move(task), std::move(done), charge_bytes, false);
  }

  bool StorageWriter::submit_ack(boost::asio::io_context& back, std::function<void(Storage &)> task,
                                 std::function<void()> done)
  {
    return enqueue(back, std::move(task), std::move(done), 0, true);
  }

  bool StorageWriter::enqueue(boost::asio::io_context& back, std::function<void(Storage &)> task,
                              std::function<void()> done, const std::size_t charge_bytes, const bool ack)
  {
    auto charge = std::make_shared<Charge>(_state, charge_bytes, ack);
    std::function < void() > item =
      [this, &back, task = std::move(task), done = std::move(done), charge]() mutable
      {
        try
        {
          task(*_writer);
        }
        catch (const std::exception& error)
        {
          std::cerr << "StorageWriter: task failed: " << error.what() << '\n';
        }
        if (done)
        {
          boost::asio::post(back, [done = std::move(done), charge] { done(); });
        }
      };
    {
      const std::lock_guard lock{_state->mutex};
      if (_state->stopping)
      {
        return false;
      }
      if (ack
            ? _state->ack_tasks >= _limits.max_ack_tasks
            : _state->request_tasks >= _limits.max_request_tasks
            || charge_bytes > _limits.max_request_bytes - _state->request_bytes)
      {
        return false;
      }
      if (ack)
      {
        ++_state->ack_tasks;
      }
      else
      {
        ++_state->request_tasks;
        _state->request_bytes += charge_bytes;
      }
      charge->arm();
      try
      {
        _state->queue.push_back(std::move(item));
      }
      catch (...)
      {
        charge->disarm();
        charge->release();
        throw;
      }
    }
    _state->wakeup.notify_one();
    return true;
  }

  void StorageWriter::stop() noexcept
  {
    {
      const std::lock_guard lock{_state->mutex};
      _state->stopping = true;
    }
    _state->wakeup.notify_one();
    if (_thread.joinable())
    {
      _thread.join();
    }
  }

  void StorageWriter::run() noexcept
  {
    for (;;)
    {
      std::function < void() > item;
      {
        std::unique_lock lock{_state->mutex};
        _state->wakeup.wait(lock, [this] { return _state->stopping || !_state->queue.empty(); });
        if (_state->queue.empty())
        {
          return;
        }
        item = std::move(_state->queue.front());
        _state->queue.pop_front();
      }
      try
      {
        item();
      }
      catch (const std::exception& error)
      {
        std::cerr << "StorageWriter: task wrapper failed: " << error.what() << '\n';
      }
      catch (...)
      {
      }
    }
  }
}
