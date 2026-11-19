#include "connector.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "message/message.hpp"

namespace cipple::tcp
{
  std::shared_ptr<Connector> Connector::start(boost::asio::io_context& context,
                                             const boost::asio::ip::tcp::endpoint endpoint,
                                             ConnectorHandlers handlers, const ConnectorOptions options)
  {
    options.session_limits.validate();
    if (options.connect_timeout <= std::chrono::steady_clock::duration::zero()
      || options.initial_retry_delay <= std::chrono::steady_clock::duration::zero()
      || options.max_retry_delay < options.initial_retry_delay)
    {
      throw std::invalid_argument{"connector: invalid connect timeout or retry delays"};
    }
    if (!handlers.on_connected || !handlers.on_message)
    {
      throw std::invalid_argument{"connector: connected and message handlers are required"};
    }
    auto connector = std::shared_ptr<Connector>{
      new Connector{context, endpoint, std::move(handlers), options}};
    connector->connect();
    return connector;
  }

  Connector::Connector(boost::asio::io_context& context, const boost::asio::ip::tcp::endpoint endpoint,
                       ConnectorHandlers handlers, const ConnectorOptions options)
    : _context{context}
      , _endpoint{endpoint}
      , _handlers{std::move(handlers)}
      , _options{options}
      , _next_retry_delay{options.initial_retry_delay}
  {
  }

  Connector::~Connector() noexcept
  {
    stop();
  }

  void Connector::stop()
  {
    if (_state == State::kStopped)
    {
      return;
    }
    _state = State::kStopped;
    _timer.reset();
    _connecting_socket.reset();
    if (auto session = std::move(_session))
    {
      session->close();
    }
  }

  void Connector::connect()
  {
    _state = State::kConnecting;
    const auto attempt = ++_attempt;
    const auto weak = weak_from_this();
    _connecting_socket.emplace(_context);
    _connecting_socket->async_connect(_endpoint, [weak, attempt](const boost::system::error_code error) {
      if (const auto self = weak.lock())
      {
        self->on_connect(attempt, error);
      }
    });
    _timer.emplace(_context, _options.connect_timeout);
    _timer->async_wait([weak, attempt](const boost::system::error_code error) {
      if (const auto self = weak.lock(); !error && self)
      {
        self->on_connect_timeout(attempt);
      }
    });
  }

  void Connector::on_connect(const std::uint64_t attempt, const boost::system::error_code error)
  {
    if (_state != State::kConnecting || _attempt != attempt)
    {
      return;
    }
    _timer.reset();
    if (error)
    {
      _connecting_socket.reset();
      wait_to_retry();
      return;
    }

    auto socket = std::move(*_connecting_socket);
    _connecting_socket.reset();
    const auto weak = weak_from_this();
    _session = Session::start(std::move(socket),
      [weak](Session& session, std::unique_ptr<message::Message> message) {
        if (const auto self = weak.lock())
        {
          self->_handlers.on_message(session, std::move(message));
        }
      },
      [weak, attempt](const boost::system::error_code reason) {
        if (const auto self = weak.lock())
        {
          self->on_session_closed(attempt, reason);
        }
      }, _options.session_limits);
    _state = State::kConnected;
    _next_retry_delay = _options.initial_retry_delay;
    _handlers.on_connected(_session);
  }

  void Connector::on_connect_timeout(const std::uint64_t attempt)
  {
    if (_state == State::kConnecting && _attempt == attempt)
    {
      _connecting_socket.reset();
      wait_to_retry();
    }
  }

  void Connector::on_session_closed(const std::uint64_t attempt, const boost::system::error_code reason)
  {
    if (_state != State::kConnected || _attempt != attempt)
    {
      return;
    }
    _session.reset();
    wait_to_retry();
    if (_handlers.on_disconnected)
    {
      _handlers.on_disconnected(reason);
    }
  }

  void Connector::wait_to_retry()
  {
    _state = State::kWaitingToRetry;
    _timer.emplace(_context, _next_retry_delay);
    _next_retry_delay += std::min(_next_retry_delay, _options.max_retry_delay - _next_retry_delay);
    _timer->async_wait([weak = weak_from_this(), attempt = _attempt](const boost::system::error_code error) {
      if (const auto self = weak.lock();
          !error && self && self->_state == State::kWaitingToRetry && self->_attempt == attempt)
      {
        self->connect();
      }
    });
  }
}
