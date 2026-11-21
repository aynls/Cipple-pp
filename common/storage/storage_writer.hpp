#pragma once
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

namespace boost::asio
{
  class io_context;
}

namespace cipple::storage
{
  class Storage;

  // Owns one write connection on a dedicated thread plus a second, read-only
  // connection handed to the caller's I/O thread. Mutations are queued as
  // tasks so commits (fsync under synchronous=FULL) never run on the I/O
  // thread; reads stay local on the caller's own connection. WAL allows
  // exactly this split: a single writer plus concurrent readers.
  class StorageWriter final
  {
  public:
    // writer and reader must be two separate connections to the same
    // database. reader() is used only for non-mutating calls.
    StorageWriter(std::unique_ptr<Storage> writer, std::unique_ptr<Storage> reader);
    ~StorageWriter() noexcept;

    StorageWriter(const StorageWriter&) = delete;
    StorageWriter& operator=(const StorageWriter&) = delete;

    // Read side of the database; call it only on the thread that submits work.
    Storage& reader() const noexcept;

    // Queues task(Storage&) on the writer thread, then posts done() onto
    // `back` once the task returns. done may be empty. Tasks run strictly in
    // submission order on the single write connection, so a task observes
    // every previously committed task. Returns false once stopped; neither
    // task nor done runs in that case.
    bool submit(boost::asio::io_context& back, std::function<void(Storage&)> task,
                std::function<void()> done = {});

    // Lets queued tasks finish, then joins the thread. Idempotent.
    void stop() noexcept;

  private:
    void run() noexcept;

    std::unique_ptr<Storage> _writer;
    std::unique_ptr<Storage> _reader;
    std::mutex _mutex;
    std::condition_variable _wakeup;
    std::deque<std::function<void()>> _queue;
    bool _stopping = false;
    std::thread _thread;
  };
}
