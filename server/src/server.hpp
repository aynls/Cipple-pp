#pragma once
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "entity/entity_message.hpp"
#include "hlc/hybrid_clock.hpp"
#include "message/result.hpp"
#include "replication.hpp"

namespace boost::asio
{
  class io_context;
}

namespace cipple::message
{
  class GetRequest;
  class Message;
  class SyncRequest;
}

namespace cipple::storage
{
  class StorageWriter;
}

namespace cipple::tcp
{
  class Session;
}

namespace cipple::server
{
  struct ServerOptions
  {
    std::int32_t node_id = 0;
    std::vector<PeerAddress> peers;
    std::int64_t clock_offset_ms = 0;
    // Mesh makes each newly durable replica responsible for further propagation.
    bool relay_peer_updates = true;
  };

  // Accepts clients and processes their requests serially on one I/O thread.
  // Mutations are committed on the storage's writer thread; responses and
  // dispatch still run on the I/O thread after the commit completes.
  class Server final : public std::enable_shared_from_this<Server>
  {
  public:
    static std::shared_ptr<Server> start(boost::asio::io_context& context,
                                        const boost::asio::ip::tcp::endpoint& endpoint,
                                        std::unique_ptr<storage::StorageWriter> storage, std::int32_t node_id = 0);
    static std::shared_ptr<Server> start(boost::asio::io_context& context,
                                        const boost::asio::ip::tcp::endpoint& endpoint,
                                        std::unique_ptr<storage::StorageWriter> storage, ServerOptions options);
    ~Server() noexcept;

    const boost::asio::ip::tcp::endpoint& local_endpoint() const noexcept;
    void stop() noexcept;

  private:
    using ConfigKey = std::pair<std::string, std::string>;

    struct PendingDelivery
    {
      Uuid reply_id;
      std::int64_t sequence;
      std::chrono::steady_clock::time_point deadline;
    };

    // Filled on the writer thread, consumed on the I/O thread.
    struct WriteOutcome
    {
      bool success = false;
      bool inserted = false;
      std::optional<std::string> error;
    };

    struct SyncOutcome
    {
      message::Result result = message::Result::kRejected;
      bool new_base = false;
      bool is_current = false;
      std::optional<std::string> error;
    };

    struct Subscription
    {
      std::optional<std::int64_t> cursor;
      std::optional<PendingDelivery> pending;
      bool needs_replay = false;
    };

    struct Connection
    {
      std::shared_ptr<tcp::Session> session;
      std::map<ConfigKey, Subscription> subscriptions;
    };

    Server(boost::asio::io_context& context, const boost::asio::ip::tcp::endpoint& endpoint,
           std::unique_ptr<storage::StorageWriter> storage, const ServerOptions& options);

    void accept_next();
    void attach(boost::asio::ip::tcp::socket socket);
    void on_message(std::uint64_t connection_id, tcp::Session& session, const message::Message& message);
    void read(tcp::Session& session, const message::GetRequest& request) const;
    void sync(tcp::Session& session, const message::SyncRequest& request);
    std::optional<entity::EntityMessage> find_base(const entity::EntityMessage& message) const;

    template<typename Response, typename Request>
    void write(tcp::Session& session, const Request& request, entity::EntityMessage::Payload payload);
    template<typename Response>
    void finish_write(tcp::Session& session, const Uuid& request_id, const entity::EntityMessage& version,
                      const WriteOutcome& outcome);
    void finish_sync(tcp::Session& session, const Uuid& request_id, const entity::EntityMessage& version,
                     const std::optional<entity::EntityMessage>& supplied, bool notify,
                     const SyncOutcome& outcome);

    void publish(const entity::EntityMessage& version, bool is_current);
    void replay(tcp::Session& session, const ConfigKey& key, Subscription& subscription);
    void check_delivery_deadlines();

    boost::asio::io_context& _context;
    std::unique_ptr<storage::StorageWriter> _storage;
    boost::asio::ip::tcp::acceptor _acceptor;
    boost::asio::steady_timer _delivery_timer;
    boost::asio::ip::tcp::endpoint _local_endpoint;
    std::int32_t _node_id;
    std::vector<std::int32_t> _peer_ids;
    bool _relay_peer_updates;
    hlc::HybridClock _clock;
    std::shared_ptr<Replication> _replication;
    std::uint64_t _next_connection_id = 0;
    std::unordered_map<std::uint64_t, Connection> _connections;
  };
}
