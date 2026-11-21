#pragma once
#include <boost/asio/ip/tcp.hpp>
#include <boost/system/error_code.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "tcp/frame_codec.hpp"

namespace cipple::tcp
{
  struct SessionLimits
  {
    std::size_t max_frame_length = kDefaultMaxFrameLength;
    // Includes each length prefix and the frame currently being written.
    std::size_t max_queued_bytes = 2 * (kDefaultMaxFrameLength + kFrameLengthPrefixSize);

    void validate() const;
  };

  // Use the socket and all callbacks on its single I/O thread. Callbacks must return normally.
  class Session final : public std::enable_shared_from_this<Session>
  {
  public:
    using MessageHandler = std::function<void(Session&, std::unique_ptr<message::Message>)>;
    using CloseHandler = std::function<void(boost::system::error_code)>;
    using WriteHandler = std::function<void(boost::system::error_code)>;

    // Takes a connected socket and starts the read loop. The session stays alive while I/O is pending.
    static std::shared_ptr<Session> start(boost::asio::ip::tcp::socket socket, MessageHandler on_message,
                                          CloseHandler on_close = {}, SessionLimits limits = {});
    ~Session() noexcept;

    // Returns the enqueue result. Accepted frames report the local write result or close reason once.
    [[nodiscard]] boost::system::error_code async_send(const message::Message& message,
                                                       WriteHandler on_written = {});
    // Sends an already encoded frame. Fan-out callers encode once and share
    // the buffer instead of paying an encode per session.
    [[nodiscard]] boost::system::error_code async_send(EncodedFrame frame,
                                                       WriteHandler on_written = {});
    void close();
    bool is_open() const noexcept;

  private:
    struct PendingWrite
    {
      EncodedFrame bytes;
      WriteHandler on_written;
    };

    Session(boost::asio::ip::tcp::socket socket, MessageHandler on_message, CloseHandler on_close,
            SessionLimits limits);

    void read_prefix();
    void on_prefix(boost::system::error_code error);
    void on_body(boost::system::error_code error);
    void write_next();
    void on_write(boost::system::error_code error);
    void finish(boost::system::error_code reason);

    boost::asio::ip::tcp::socket _socket;
    SessionLimits _limits;
    MessageHandler _on_message;
    CloseHandler _on_close;
    std::array<std::uint8_t, kFrameLengthPrefixSize> _prefix{};
    std::vector<std::uint8_t> _body;
    std::deque<PendingWrite> _pending_writes;
    std::optional<PendingWrite> _active_write;
    std::size_t _queued_bytes = 0;
    bool _closed = false;
  };
}
