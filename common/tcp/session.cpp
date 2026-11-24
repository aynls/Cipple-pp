#include "session.hpp"

#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include <stdexcept>
#include <utility>

#include "protocol/message.hpp"
#include "protocol/message_decoder.hpp"

namespace cipple::tcp
{
  void SessionLimits::validate() const
  {
    validate_frame_limit(max_frame_length);
    if (max_queued_bytes == 0)
    {
      throw std::invalid_argument{"session: max queued bytes must be positive"};
    }
  }

  std::shared_ptr<Session> Session::start(boost::asio::ip::tcp::socket socket, MessageHandler on_message,
                                          CloseHandler on_close, const SessionLimits limits)
  {
    limits.validate();
    if (!socket.is_open() || !on_message)
    {
      throw std::invalid_argument{"session: a connected socket and a message handler are required"};
    }
    auto session{
      std::shared_ptr<Session>{
        new Session{std::move(socket), std::move(on_message), std::move(on_close), limits}
      }
    };
    session->read_prefix();
    return session;
  }

  Session::Session(boost::asio::ip::tcp::socket socket, MessageHandler on_message, CloseHandler on_close,
                   const SessionLimits limits)
    : _socket{std::move(socket)}
      , _limits{limits}
      , _on_message{std::move(on_message)}
      , _on_close{std::move(on_close)}
  {
  }

  Session::~Session() noexcept = default;

  boost::system::error_code Session::async_send(const message::Message& message, WriteHandler on_written)
  {
    EncodedFrame frame;
    try
    {
      frame = encode_frame_shared(message, _limits.max_frame_length);
    }
    catch (const std::length_error&)
    {
      return boost::asio::error::message_size;
    }
    return async_send(std::move(frame), std::move(on_written));
  }

  boost::system::error_code Session::async_send(EncodedFrame frame, WriteHandler on_written)
  {
    if (_closed)
    {
      return boost::asio::error::not_connected;
    }
    if (!frame || frame->size() < kFrameLengthPrefixSize)
    {
      return boost::asio::error::invalid_argument;
    }
    if (frame->size() - kFrameLengthPrefixSize > _limits.max_frame_length)
    {
      return boost::asio::error::message_size;
    }
    if (frame->size() > _limits.max_queued_bytes - _queued_bytes)
    {
      return boost::asio::error::no_buffer_space;
    }
    const auto size{frame->size()};
    _pending_writes.push_back({std::move(frame), std::move(on_written)});
    _queued_bytes += size;
    write_next();
    return {};
  }

  void Session::close()
  {
    const auto self{shared_from_this()};
    finish(boost::asio::error::operation_aborted);
  }

  bool Session::is_open() const noexcept
  {
    return !_closed;
  }

  void Session::read_prefix()
  {
    boost::asio::async_read(_socket, boost::asio::buffer(_prefix),
                            [self{shared_from_this()}](const boost::system::error_code error, std::size_t)
                            {
                              self->on_prefix(error);
                            });
  }

  void Session::on_prefix(const boost::system::error_code error)
  {
    if (_closed)
    {
      return;
    }
    if (error)
    {
      finish(error);
      return;
    }

    std::size_t body_length;
    try
    {
      body_length = decode_frame_length(_prefix, _limits.max_frame_length);
    }
    catch (const std::length_error&)
    {
      finish(boost::asio::error::message_size);
      return;
    }
    catch (const std::invalid_argument&)
    {
      finish(make_error_code(boost::system::errc::protocol_error));
      return;
    }

    _body.resize(body_length);
    boost::asio::async_read(_socket, boost::asio::buffer(_body),
                            [self{shared_from_this()}](const boost::system::error_code read_error, std::size_t)
                            {
                              self->on_body(read_error);
                            });
  }

  void Session::on_body(const boost::system::error_code error)
  {
    if (_closed)
    {
      return;
    }
    if (error)
    {
      finish(error);
      return;
    }

    std::unique_ptr<message::Message> message;
    try
    {
      message = decode_message(_body);
    }
    catch (const std::logic_error&)
    {
      finish(make_error_code(boost::system::errc::protocol_error));
      return;
    }

    // The message handler may close the session while it is executing.
    auto handler{std::move(_on_message)};
    handler(*this, std::move(message));
    if (!_closed)
    {
      _on_message = std::move(handler);
      read_prefix();
    }
  }

  void Session::write_next()
  {
    if (_closed || _active_write || _pending_writes.empty())
    {
      return;
    }
    _active_write.emplace(std::move(_pending_writes.front()));
    _pending_writes.pop_front();
    boost::asio::async_write(_socket, boost::asio::buffer(*_active_write->bytes),
                             [self{shared_from_this()}](const boost::system::error_code error, std::size_t)
                             {
                               self->on_write(error);
                             });
  }

  void Session::on_write(const boost::system::error_code error)
  {
    if (_closed)
    {
      _active_write.reset();
      return;
    }
    if (error)
    {
      finish(error);
      _active_write.reset();
      return;
    }

    auto on_written{std::move(_active_write->on_written)};
    _queued_bytes -= _active_write->bytes->size();
    _active_write.reset();
    if (on_written)
    {
      on_written({});
    }
    write_next();
  }

  void Session::finish(const boost::system::error_code reason)
  {
    if (_closed)
    {
      return;
    }
    _closed = true;
    boost::system::error_code ignored;
    _socket.close(ignored);
    _on_message = {};

    // The in-flight buffer stays valid until the cancelled write completes.
    if (_active_write&& _active_write->on_written)
    {
      auto on_written{std::move(_active_write->on_written)};
      on_written(reason);
    }
    while (!_pending_writes.empty())
    {
      auto on_written{std::move(_pending_writes.front().on_written)};
      _pending_writes.pop_front();
      if (on_written)
      {
        on_written(reason);
      }
    }
    _queued_bytes = 0;
    if (auto on_close{std::move(_on_close)})
    {
      on_close(reason);
    }
  }
}
