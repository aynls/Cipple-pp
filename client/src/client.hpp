#pragma once
#include <boost/asio/ip/tcp.hpp>
#include <boost/system/error_code.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "uuid.hpp"

namespace boost::asio
{
  class io_context;
}

namespace cipple::entity
{
  class EntityMessage;
}

namespace cipple::message
{
  class Message;
  enum class MessageType : std::uint8_t;
}

namespace cipple::tcp
{
  class Connector;
  class Session;
}

namespace cipple::client
{
  struct ClientHandlers
  {
    std::function<void()> on_connected;
    std::function<void(boost::system::error_code)> on_disconnected;
    // The version is borrowed for the duration of this callback.
    std::function<void(const entity::EntityMessage&)> on_update;
  };

  // Use on one I/O thread. Handlers must return normally.
  class Client final : public std::enable_shared_from_this<Client>
  {
  public:
    // An empty error code means the server replied. The bool is the server's result.
    using StatusHandler = std::function<void(boost::system::error_code, bool)>;
    // A successful GET reports std::nullopt for an absent or deleted key.
    using GetHandler = std::function<void(boost::system::error_code, std::optional<std::string>)>;

    static std::shared_ptr<Client> start(boost::asio::io_context& context,
                                        const boost::asio::ip::tcp::endpoint& endpoint, ClientHandlers handlers = {});
    ~Client() noexcept;

    bool is_connected() const noexcept;
    void stop();

    // Use the same UUID when retrying a write with an uncertain result.
    void put(std::string application_name, std::string key, std::string value, StatusHandler on_done,
             Uuid uuid = Uuid::generate());
    void erase(std::string application_name, std::string key, StatusHandler on_done, Uuid uuid = Uuid::generate());
    void get(std::string application_name, std::string key, GetHandler on_done);

    // Subscriptions belong to the current session. on_connected runs for each new session.
    void subscribe(std::string application_name, std::string key, StatusHandler on_done);
    void unsubscribe(std::string application_name, std::string key, StatusHandler on_done);

  private:
    using ResponseHandler = std::function<void(boost::system::error_code, const message::Message*)>;

    struct PendingResponse
    {
      message::MessageType type;
      ResponseHandler on_done;
    };

    explicit Client(ClientHandlers handlers);

    void send(const message::Message& request, message::MessageType response_type, ResponseHandler on_done);
    void on_message(tcp::Session& session, const message::Message& message);
    void fail_pending(boost::system::error_code reason);

    ClientHandlers _handlers;
    std::unordered_map<Uuid, PendingResponse> _pending;
    std::shared_ptr<tcp::Session> _session;
    std::shared_ptr<tcp::Connector> _connector;
  };
}
