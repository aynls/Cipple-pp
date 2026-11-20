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
  class Storage;
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
  class Server final : public std::enable_shared_from_this<Server>
  {
  public:
    static std::shared_ptr<Server> start(boost::asio::io_context& context,
                                        const boost::asio::ip::tcp::endpoint& endpoint,
                                        std::unique_ptr<storage::Storage> storage, std::int32_t node_id = 0);
    static std::shared_ptr<Server> start(boost::asio::io_context& context,
                                        const boost::asio::ip::tcp::endpoint& endpoint,
                                        std::unique_ptr<storage::Storage> storage, ServerOptions options);
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
           std::unique_ptr<storage::Storage> storage, const ServerOptions& options);

    void accept_next();
    void attach(boost::asio::ip::tcp::socket socket);
    void on_message(std::uint64_t connection_id, tcp::Session& session, const message::Message& message);
    void read(tcp::Session& session, const message::GetRequest& request) const;
    void sync(tcp::Session& session, const message::SyncRequest& request);
    std::optional<entity::EntityMessage> find_base(const entity::EntityMessage& message) const;

    template<typename Response, typename Request>
    void write(tcp::Session& session, const Request& request, entity::EntityMessage::Payload payload);

    void publish(const entity::EntityMessage& version, bool is_current);
    void replay(tcp::Session& session, const ConfigKey& key, Subscription& subscription);
    void check_delivery_deadlines();

    std::unique_ptr<storage::Storage> _storage;
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
