#include "storage/storage_writer.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>

#include <exception>
#include <iostream>
#include <stdexcept>
#include <utility>

#include "storage/storage.hpp"

namespace cipple::storage
{
  StorageWriter::StorageWriter(std::unique_ptr<Storage> writer, std::unique_ptr<Storage> reader)
    : _writer{std::move(writer)}
      , _reader{std::move(reader)}
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

  bool StorageWriter::submit(boost::asio::io_context& back, std::function<void(Storage&)> task,
                             std::function<void()> done)
  {
    {
      const std::lock_guard lock{_mutex};
      if (_stopping)
      {
        return false;
      }
      _queue.push_back([this, &back, task = std::move(task), done = std::move(done)]() mutable {
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
          boost::asio::post(back, std::move(done));
        }
      });
    }
    _wakeup.notify_one();
    return true;
  }

  void StorageWriter::stop() noexcept
  {
    {
      const std::lock_guard lock{_mutex};
      _stopping = true;
    }
    _wakeup.notify_one();
    if (_thread.joinable())
    {
      _thread.join();
    }
  }

  void StorageWriter::run() noexcept
  {
    for (;;)
    {
      std::function<void()> item;
      {
        std::unique_lock lock{_mutex};
        _wakeup.wait(lock, [this] { return _stopping || !_queue.empty(); });
        if (_queue.empty())
        {
          return;
        }
        item = std::move(_queue.front());
        _queue.pop_front();
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
