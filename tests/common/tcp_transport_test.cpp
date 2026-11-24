#include <gtest/gtest.h>
#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "protocol/configuration/put_request.hpp"
#include "tcp/connector.hpp"
#include "tcp/session.hpp"

namespace
{
  namespace asio = boost::asio;
  using Socket = asio::ip::tcp::socket;
  using boost::system::error_code;
  using cipple::Uuid;
  using cipple::message::Message;
  using cipple::message::PutRequest;
  using cipple::tcp::Connector;
  using cipple::tcp::Session;
  using cipple::tcp::SessionLimits;
  using cipple::tcp::encode_frame;
  using namespace std::chrono_literals;

  class TcpTransportTest : public testing::Test
  {
  protected:
    template <typename Predicate>
    bool run_until(Predicate complete)
    {
      if (_io.stopped())
      {
        _io.restart();
      }
      const auto deadline = std::chrono::steady_clock::now() + 3s;
      while (!complete() && std::chrono::steady_clock::now() < deadline)
      {
        if (_io.run_one_until(deadline) == 0)
        {
          break;
        }
      }
      return complete();
    }

    void run_for(const std::chrono::milliseconds duration)
    {
      if (_io.stopped())
      {
        _io.restart();
      }
      _io.run_for(duration);
    }

    asio::io_context _io;
    asio::ip::tcp::acceptor _listener{_io, {asio::ip::address_v4::loopback(), 0}};
  };

  TEST_F(TcpTransportTest, EchoesSplitAndCoalescedFramesAndRejectsAnOversizedFrame)
  {
    Socket peer{_io};
    peer.connect(_listener.local_endpoint());

    const auto first = encode_frame(PutRequest{Uuid::generate(), "app", "key", "first"});
    const auto second = encode_frame(PutRequest{Uuid::generate(), "app", "key", "second"});
    auto expected_echo = first;
    expected_echo.insert(expected_echo.end(), second.begin(), second.end());
    std::size_t received = 0;
    std::vector<error_code> closes;
    const auto session = Session::start(_listener.accept(),
                                        [&](Session& connection, std::unique_ptr<Message> message)
                                        {
                                          ++received;
                                          EXPECT_FALSE(connection.async_send(*message));
                                        },
                                        [&](error_code reason) { closes.push_back(reason); },
                                        SessionLimits{.max_frame_length = 128});

    asio::write(peer, asio::buffer(first.data(), 2));
    run_for(20ms);
    EXPECT_EQ(received, 0);
    asio::write(peer, asio::buffer(first.data() + 2, 8));
    run_for(20ms);
    EXPECT_EQ(received, 0);
    asio::write(peer, asio::buffer(expected_echo.data() + 10, expected_echo.size() - 10));

    std::vector<std::uint8_t> echo(expected_echo.size());
    std::optional<error_code> echo_result;
    asio::async_read(peer, asio::buffer(echo), [&](error_code error, std::size_t) { echo_result = error; });
    ASSERT_TRUE(run_until([&] { return echo_result.has_value(); }));
    ASSERT_FALSE(*echo_result);
    EXPECT_EQ(received, 2);
    EXPECT_EQ(echo, expected_echo);

    const std::array<std::uint8_t, 4> oversized_prefix{0, 0, 0, 129};
    asio::write(peer, asio::buffer(oversized_prefix));
    ASSERT_TRUE(run_until([&] { return !closes.empty(); }));
    EXPECT_EQ(received, 2);
    session->close();
    ASSERT_EQ(closes.size(), 1);
    EXPECT_EQ(closes.front(), asio::error::message_size);
  }

  TEST_F(TcpTransportTest, BoundsTheSendQueueAndCompletesCancelledWritesOnce)
  {
    Socket peer{_io};
    peer.connect(_listener.local_endpoint());
    const PutRequest message{Uuid::generate(), "app", "key", "value"};
    std::vector<error_code> writes;
    const auto session = Session::start(_listener.accept(), [](Session&, std::unique_ptr<Message>)
                                        {
                                        }, {},
                                        SessionLimits{.max_queued_bytes = 2 * encode_frame(message).size()});
    const auto on_written = [&](error_code error) { writes.push_back(error); };

    ASSERT_FALSE(session->async_send(message, on_written));
    ASSERT_FALSE(session->async_send(message, on_written));
    EXPECT_EQ(session->async_send(message, on_written), asio::error::no_buffer_space);
    session->close();
    session->close();
    EXPECT_EQ(session->async_send(message), asio::error::not_connected);
    run_for(100ms);

    ASSERT_EQ(writes.size(), 2);
    EXPECT_EQ(writes[0], asio::error::operation_aborted);
    EXPECT_EQ(writes[1], asio::error::operation_aborted);
  }

  TEST_F(TcpTransportTest, ExchangesMessagesAfterReconnectAndCancelsTheNextRetry)
  {
    std::vector<std::shared_ptr<Session>> clients;
    std::vector<std::shared_ptr<Session>> servers;
    std::vector<Uuid> replies;
    std::vector<error_code> disconnects;
    const auto accept_echo = [&]
    {
      _listener.async_accept([&](error_code error, Socket socket)
      {
        ASSERT_FALSE(error);
        servers.push_back(Session::start(std::move(socket),
                                         [](Session& session, std::unique_ptr<Message> message)
                                         {
                                           EXPECT_FALSE(session.async_send(*message));
                                         }));
      });
    };

    accept_echo();
    const auto connector = Connector::start(_io, _listener.local_endpoint(), {
                                              .on_connected = [&](std::shared_ptr<Session> session)
                                              {
                                                clients.push_back(std::move(session));
                                              },
                                              .on_message = [&](Session&, std::unique_ptr<Message> message)
                                              {
                                                replies.push_back(message->uuid());
                                              },
                                              .on_disconnected = [&](error_code reason)
                                              {
                                                disconnects.push_back(reason);
                                              },
                                            }, {.initial_retry_delay = 20ms, .max_retry_delay = 40ms});
    ASSERT_TRUE(run_until([&] { return clients.size() == 1 && servers.size() == 1; }));
    const PutRequest first{Uuid::generate(), "app", "key", "before"};
    ASSERT_FALSE(clients.front()->async_send(first));
    ASSERT_TRUE(run_until([&] { return !replies.empty(); }));
    EXPECT_EQ(replies.front(), first.uuid());

    accept_echo();
    servers.front()->close();
    ASSERT_TRUE(run_until([&] { return clients.size() == 2 && servers.size() == 2; }));
    ASSERT_EQ(disconnects.size(), 1);
    EXPECT_TRUE(disconnects.front());
    EXPECT_FALSE(clients.front()->is_open());
    EXPECT_NE(clients.front(), clients.back());
    const PutRequest second{Uuid::generate(), "app", "key", "after"};
    ASSERT_FALSE(clients.back()->async_send(second));
    ASSERT_TRUE(run_until([&] { return replies.size() >= 2; }));
    EXPECT_EQ(replies, (std::vector<Uuid>{first.uuid(), second.uuid()}));

    servers.back()->close();
    ASSERT_TRUE(run_until([&] { return disconnects.size() >= 2; }));
    connector->stop();
    run_for(200ms);
    EXPECT_EQ(clients.size(), 2);
    EXPECT_EQ(disconnects.size(), 2);
  }
}
