#pragma once
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>

namespace cipple::message
{
  class Message;
}

namespace cipple::raft
{
  class ConfigurationMachine;
  class RaftNode;
  struct RaftOptions;
}

namespace cipple::tcp
{
  class Session;
}

namespace cipple::sharding
{
  class ShardLayout;
}

namespace cipple::server
{
  class Subscriptions;

  struct ShardOwnership
  {
    std::uint32_t shard_id;
    std::shared_ptr<const sharding::ShardLayout> layout;
  };

  struct RelayRecoveryOptions
  {
    std::uint64_t bytes_per_second{8 * 1024 * 1024};
    std::uint64_t burst_bytes{32 * 1024 * 1024};

    void validate() const;
  };

  // Application sockets and subscriptions are owned by one I/O thread.
  // Raft persists and applies commands on its own threads before posting a reply.
  class Server final : public std::enable_shared_from_this<Server>
  {
  public:
    static std::shared_ptr<Server> start(boost::asio::io_context& context,
                                         const boost::asio::ip::tcp::endpoint& endpoint,
                                         const std::filesystem::path& database, raft::RaftOptions options,
                                         std::optional<ShardOwnership> shard = std::nullopt,
                                         RelayRecoveryOptions recovery = {});
    ~Server() noexcept;

    const boost::asio::ip::tcp::endpoint& local_endpoint() const noexcept;
    void stop() noexcept;

  private:
    class DatabaseLock;

    Server(boost::asio::io_context& context, const boost::asio::ip::tcp::endpoint& endpoint);

    void accept_next();
    void attach(boost::asio::ip::tcp::socket socket);
    void on_message(std::uint64_t connection_id, tcp::Session& session, const message::Message& message);
    std::optional<std::uint32_t> wrong_shard(const message::Message& message) const noexcept;
    void submit(tcp::Session& session, const message::Message& command);
    void schedule_replay() noexcept;
    void schedule_recovery() noexcept;
    void check_delivery_deadlines();

    boost::asio::io_context& _context;
    std::unique_ptr<DatabaseLock> _database_lock;
    std::optional<ShardOwnership> _shard;
    std::shared_ptr<raft::ConfigurationMachine> _machine;
    std::unique_ptr<Subscriptions> _subscriptions;
    std::unique_ptr<raft::RaftNode> _raft;
    boost::asio::ip::tcp::acceptor _acceptor;
    boost::asio::steady_timer _delivery_timer;
    boost::asio::steady_timer _recovery_timer;
    boost::asio::ip::tcp::endpoint _local_endpoint;
    std::uint64_t _next_connection_id{0};
    std::size_t _pending_reads{0};
    bool _replay_scheduled{false};
  };
}
