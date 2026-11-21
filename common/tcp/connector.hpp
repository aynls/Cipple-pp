#pragma once
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "tcp/session.hpp"

namespace cipple::tcp
{
  struct ConnectorOptions
  {
    std::chrono::steady_clock::duration connect_timeout = std::chrono::seconds{5};
    std::chrono::steady_clock::duration initial_retry_delay = std::chrono::milliseconds{100};
    std::chrono::steady_clock::duration max_retry_delay = std::chrono::seconds{5};
    SessionLimits session_limits{};
  };

  struct ConnectorHandlers
  {
    std::function<void(std::shared_ptr<Session>)> on_connected;
    Session::MessageHandler on_message;
    // Reports loss of an established session while the connector is running.
    Session::CloseHandler on_disconnected{};
  };

  // Retain the connector while it is running. Use and release it on the context's
  // single I/O thread. Callbacks must return normally.
  class Connector final : public std::enable_shared_from_this<Connector>
  {
  public:
    static std::shared_ptr<Connector> start(boost::asio::io_context& context,
                                            boost::asio::ip::tcp::endpoint endpoint,
                                            ConnectorHandlers handlers, ConnectorOptions options = {});
    // Tries endpoints in the supplied order, rotating after failures.
    static std::shared_ptr<Connector> start_any(boost::asio::io_context& context,
                                                std::vector<boost::asio::ip::tcp::endpoint> endpoints,
                                                ConnectorHandlers handlers, ConnectorOptions options = {});
    ~Connector() noexcept;

    const boost::asio::ip::tcp::endpoint& endpoint() const noexcept;

    // Cancels the current attempt or retry and closes the active session.
    void stop();

  private:
    enum class State
    {
      kConnecting,
      kConnected,
      kWaitingToRetry,
      kStopped,
    };

    Connector(boost::asio::io_context& context, std::vector<boost::asio::ip::tcp::endpoint> endpoints,
              ConnectorHandlers handlers, ConnectorOptions options);

    void connect();
    void on_connect(std::uint64_t attempt, boost::system::error_code error);
    void on_connect_timeout(std::uint64_t attempt);
    void on_session_closed(std::uint64_t attempt, boost::system::error_code reason);
    void wait_to_retry();

    boost::asio::io_context& _context;
    std::vector<boost::asio::ip::tcp::endpoint> _endpoints;
    std::size_t _endpoint_index = 0;
    std::size_t _attempts_in_round = 0;
    ConnectorHandlers _handlers;
    ConnectorOptions _options;
    std::chrono::steady_clock::duration _next_retry_delay;
    std::optional<boost::asio::ip::tcp::socket> _connecting_socket;
    std::optional<boost::asio::steady_timer> _timer;
    std::shared_ptr<Session> _session;
    std::uint64_t _attempt = 0;
    State _state = State::kWaitingToRetry;
  };
}
