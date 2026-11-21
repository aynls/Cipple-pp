#pragma once
#include <condition_variable>
#include <cstddef>
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
    // Bounds the work a submitter can pin. A request charge (one task slot
    // plus charge_bytes) is held from submit() until the task ran AND its
    // done callback was consumed or destroyed on `back`; an empty done
    // releases at task end. Queued, running and posted-but-unconsumed work
    // therefore all count against the limits. Ack submissions draw from
    // max_ack_tasks only, so request traffic can never starve replication
    // bookkeeping. The defaults hold several maximum-frame payloads.
    struct Limits
    {
      std::size_t max_request_tasks = 1024;
      std::size_t max_request_bytes = 64u << 20;
      std::size_t max_ack_tasks = 256;
    };

    // writer and reader must be two separate connections to the same
    // database. reader() is used only for non-mutating calls.
    StorageWriter(std::unique_ptr<Storage> writer, std::unique_ptr<Storage> reader);
    StorageWriter(std::unique_ptr<Storage> writer, std::unique_ptr<Storage> reader, Limits limits);
    ~StorageWriter() noexcept;

    StorageWriter(const StorageWriter&) = delete;
    StorageWriter& operator=(const StorageWriter&) = delete;

    // Read side of the database; call it only on the thread that submits work.
    Storage& reader() const noexcept;

    // Queues task(Storage&) on the writer thread, then posts done() onto
    // `back` once the task returns. done may be empty. Tasks run strictly
    // in submission order on the single write connection, so a task
    // observes every previously committed task. Returns false once the
    // request lane is full or the writer is stopped; neither task nor done
    // is accepted then. charge_bytes estimates the payload the task and
    // done capture; it stays charged until the done runs (or is destroyed)
    // on `back`. `back` must stay valid until stop() has drained every task
    // that posts to it.
    bool submit(boost::asio::io_context& back, std::function<void(Storage&)> task,
                std::function<void()> done = {}, std::size_t charge_bytes = 0);

    // Like submit() but charges the reserved ack lane, which is limited by
    // task count alone. Use it for small bookkeeping commits (for example
    // replication acknowledgements) whose progress must not depend on
    // request traffic making room first.
    bool submit_ack(boost::asio::io_context& back, std::function<void(Storage&)> task,
                    std::function<void()> done = {});

    // Lets queued tasks finish and posts their done callbacks, then joins
    // the thread. When stop() returns, every accepted task has run and
    // every done has been posted to `back`; dones already posted there may
    // still be pending and are not waited on. Idempotent.
    void stop() noexcept;

  private:
    struct SharedState;
    class Charge;

    bool enqueue(boost::asio::io_context& back, std::function<void(Storage&)> task,
                 std::function<void()> done, std::size_t charge_bytes, bool ack);
    void run() noexcept;

    const Limits _limits;
    std::unique_ptr<Storage> _writer;
    std::unique_ptr<Storage> _reader;
    // Shared with in-flight charges so posted callbacks stay safe to
    // release even after this StorageWriter is destroyed.
    std::shared_ptr<SharedState> _state;
    std::thread _thread;
  };
}
