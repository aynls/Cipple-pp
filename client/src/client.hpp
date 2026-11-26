#pragma once
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/system/error_code.hpp>

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "uuid.hpp"

namespace boost::asio
{
  class io_context;
}

namespace cipple::entity
{
  class EntityMessage;
  enum class AtomicOperation : std::uint8_t;
}

namespace cipple::message
{
  class DispatchRequest;
  class GetResponse;
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
  enum class ClientError
  {
    kWrongShard = 1,
  };

  boost::system::error_code make_error_code(ClientError error) noexcept;

  struct ClientHandlers
  {
    std::function<void()> on_connected;
    std::function<void(boost::system::error_code)> on_disconnected;
    // The version is borrowed for the duration of this callback.
    std::function<void(const entity::EntityMessage&)> on_update;
    // When set, handles the delivery in place of on_update. Return true only
    // after accepting the update, so the session can send a successful receipt.
    std::function<bool(const boost::asio::ip::tcp::endpoint &, const message::DispatchRequest &)> on_delivery{};
  };

  struct ClientOptions
  {
    std::chrono::steady_clock::duration request_timeout{std::chrono::seconds{10}};
    std::chrono::steady_clock::duration connect_timeout{std::chrono::seconds{5}};
    // When set, send a heartbeat after this interval and allow one interval for its reply.
    std::optional<std::chrono::steady_clock::duration> heartbeat_interval{std::nullopt};
  };

  // Use on one I/O thread. Handlers must return normally.
  class Client final : public std::enable_shared_from_this<Client>
  {
  public:
    // An empty error code means the server replied. The bool is the server's result.
    using StatusHandler = std::function<void(boost::system::error_code, bool)>;
    // A successful GET reports std::nullopt for an absent or deleted key.
    using GetHandler = std::function<void(boost::system::error_code, std::optional<std::string>)>;
    using GetBaseHandler = std::function<void(boost::system::error_code, std::optional<Uuid>)>;
    using HistoryLostHandler = std::function<void(std::int64_t minimum_cursor)>;

    static std::shared_ptr<Client> start(boost::asio::io_context& context,
                                         const boost::asio::ip::tcp::endpoint& endpoint, ClientHandlers handlers = {});
    // Starts at the first endpoint. A connection failure or loss advances through the list.
    static std::shared_ptr<Client> start_any(boost::asio::io_context& context,
                                             std::vector<boost::asio::ip::tcp::endpoint> endpoints,
                                             ClientHandlers handlers = {}, ClientOptions options = {});
    ~Client() noexcept;

    bool is_connected() const noexcept;
    std::optional<boost::asio::ip::tcp::endpoint> remote_endpoint() const noexcept;
    void stop();

    // After an uncertain write, reuse its UUID and original content on the same cluster.
    void put(std::string application_name, std::string key, std::string value, StatusHandler on_done,
             Uuid uuid = Uuid::generate());
    void erase(std::string application_name, std::string key, StatusHandler on_done, Uuid uuid = Uuid::generate());
    void incremental_update(std::string application_name, std::string key, Uuid base_message_uuid,
                            entity::AtomicOperation operation, std::string value, StatusHandler on_done,
                            Uuid uuid = Uuid::generate());
    void get(std::string application_name, std::string key, GetHandler on_done);
    void get_base(std::string application_name, std::string key, GetBaseHandler on_done);

    // Subscriptions belong to the current session. on_connected runs for each new session.
    // resume_after enables append-order delivery starting after this cluster cursor.
    void subscribe(std::string application_name, std::string key, StatusHandler on_done,
                   std::optional<std::int64_t> resume_after = std::nullopt,
                   HistoryLostHandler on_history_lost = {});
    void unsubscribe(std::string application_name, std::string key, StatusHandler on_done);

  private:
    using ResponseHandler = std::function<void(boost::system::error_code, const message::Message *)>;

    struct PendingResponse
    {
      message::MessageType type;
      ResponseHandler on_done;
      std::uint64_t generation;
      boost::asio::steady_timer deadline;
      std::shared_ptr<const std::vector<std::uint8_t>> frame;
      bool retryable;
      bool in_flight{false};
    };

    Client(boost::asio::io_context& context, ClientHandlers handlers, ClientOptions options);

    void get_response(std::string application_name, std::string key,
                      std::function<void(boost::system::error_code, const message::GetResponse *)> on_done);
    void send(const message::Message& request, message::MessageType response_type, ResponseHandler on_done,
              std::optional<std::chrono::steady_clock::duration> timeout = std::nullopt);
    boost::system::error_code transmit(PendingResponse& pending) const;
    void transmit_pending();
    void schedule_heartbeat();
    void send_heartbeat();
    void on_message(tcp::Session& session, const message::Message& message);
    void on_request_timeout(const Uuid& uuid, std::uint64_t generation);
    void fail_session_pending(boost::system::error_code reason);
    void fail_pending(boost::system::error_code reason);

    boost::asio::io_context& _context;
    ClientHandlers _handlers;
    std::chrono::steady_clock::duration _request_timeout;
    std::optional<std::chrono::steady_clock::duration> _heartbeat_interval;
    std::optional<boost::asio::steady_timer> _heartbeat_timer;
    std::uint64_t _request_generation{0};
    std::unordered_map<Uuid, PendingResponse> _pending;
    std::shared_ptr<tcp::Session> _session;
    std::shared_ptr<tcp::Connector> _connector;
  };
}

namespace boost::system
{
  template <>
  struct is_error_code_enum<cipple::client::ClientError> : std::true_type
  {
  };
}
