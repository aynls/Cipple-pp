#pragma once
#include <boost/asio/ip/tcp.hpp>

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>

#include "entity/entity_message.hpp"

namespace boost::asio
{
  class io_context;
}

namespace cipple::message
{
  class GetRequest;
  class Message;
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
  // Accepts clients and processes their requests serially on one I/O thread.
  class Server final : public std::enable_shared_from_this<Server>
  {
  public:
    static std::shared_ptr<Server> start(boost::asio::io_context& context,
                                        const boost::asio::ip::tcp::endpoint& endpoint,
                                        std::unique_ptr<storage::Storage> storage, std::int32_t node_id = 0);
    ~Server() noexcept;

    const boost::asio::ip::tcp::endpoint& local_endpoint() const noexcept;
    void stop() noexcept;

  private:
    using ConfigKey = std::pair<std::string, std::string>;

    struct Connection
    {
      std::shared_ptr<tcp::Session> session;
      std::set<ConfigKey> subscriptions;
    };

    Server(boost::asio::io_context& context, const boost::asio::ip::tcp::endpoint& endpoint,
           std::unique_ptr<storage::Storage> storage, std::int32_t node_id);

    void accept_next();
    void attach(boost::asio::ip::tcp::socket socket);
    void on_message(std::uint64_t connection_id, tcp::Session& session, const message::Message& message);
    void read(tcp::Session& session, const message::GetRequest& request) const;

    template<typename Response, typename Request>
    void write(tcp::Session& session, const Request& request, entity::EntityMessage::Payload payload);

    void publish(entity::EntityMessage version);

    std::unique_ptr<storage::Storage> _storage;
    boost::asio::ip::tcp::acceptor _acceptor;
    boost::asio::ip::tcp::endpoint _local_endpoint;
    std::int32_t _node_id;
    std::uint64_t _next_connection_id = 0;
    std::unordered_map<std::uint64_t, Connection> _connections;
  };
}
