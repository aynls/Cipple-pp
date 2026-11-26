#include "benches.hpp"

#include <boost/asio.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "entity/entity_message.hpp"
#include "protocol/subscription/dispatch_request.hpp"
#include "protocol/message.hpp"
#include "protocol/frame_codec.hpp"
#include "tcp/session.hpp"
#include "uuid.hpp"

namespace
{
  namespace asio = boost::asio;
  using Clock = std::chrono::steady_clock;

  // 模拟订阅者侧：只读帧并计数到达字节，达到当次配额即记录完成时刻。
  struct Client
  {
    explicit Client(asio::io_context& io) : socket(io)
    {
    }

    asio::ip::tcp::socket socket;
    std::array<char, 64 * 1024> buffer{};
    std::size_t quota{0};
    std::size_t received{0};
    std::size_t errors{0};
    Clock::time_point done_at{};
  };

  double pct(std::vector<double>& sorted, double q)
  {
    if (sorted.empty())
    {
      return 0;
    }
    std::sort(sorted.begin(), sorted.end());
    return sorted[std::min<std::size_t>(static_cast<std::size_t>(q * sorted.size()), sorted.size() - 1)];
  }
}

namespace cipple::bench
{
  // loopback 上 N 对 Session/裸 socket：服务端 io_context 只跑发送循环，
  // 客户端 io_context 只跑接收，互不污染。同一二进制内直接对比
  // “每会话 encode”（旧）与“共享帧”（新）两条真实发送路径。
  int fanout(std::span<const char* const> args)
  {
    const auto subs{arg_count(args, "--subs", 500)};
    const auto ops{arg_count(args, "--ops", 40)};
    const auto warmup{arg_count(args, "--warmup", 5)};
    const auto value_bytes{arg_count(args, "--value-bytes", 256)};

    const entity::EntityMessage entity{
      Uuid::generate(), "bench-app", "bench-key",
      1'700'000'000'000, 1,
      entity::EntityMessage::Update{std::string(value_bytes, 'x')}
    };
    const message::DispatchRequest dispatch{Uuid::generate(), entity, 7};
    const auto frame_size{tcp::encode_frame(dispatch).size()};

    asio::io_context server_io, client_io;
    asio::ip::tcp::acceptor acceptor{server_io, {asio::ip::address_v4::loopback(), 0}};
    const auto endpoint{acceptor.local_endpoint()};

    std::vector<std::unique_ptr<Client>> clients;
    std::vector<std::shared_ptr<tcp::Session>> sessions;
    std::atomic<std::size_t> connected{0}, accepted{0}, done{0}, send_errors{0};

    clients.reserve(subs);
    for (std::size_t i{0}; i < subs; ++i)
    {
      clients.push_back(std::make_unique<Client>(client_io));
    }

    std::function < void(Client &) > read_next;
    read_next = [&](Client& c)
    {
      c.socket.async_read_some(asio::buffer(c.buffer), [&](boost::system::error_code ec, std::size_t n)
      {
        if (ec)
        {
          ++c.errors;
          return;
        }
        c.received += n;
        if (c.received == c.quota)
        {
          c.done_at = Clock::now();
          done.fetch_add(1, std::memory_order::release);
        }
        read_next(c);
      });
    };

    for (std::size_t i{0}; i < subs; ++i)
    {
      Client& c{*clients[i]};
      c.socket.async_connect(endpoint, [&](boost::system::error_code ec)
      {
        if (ec)
        {
          ++c.errors;
          return;
        }
        read_next(c);
        connected.fetch_add(1, std::memory_order::release);
      });
      acceptor.async_accept([&](boost::system::error_code ec, asio::ip::tcp::socket socket)
      {
        if (ec)
        {
          return;
        }
        sessions.push_back(tcp::Session::start(std::move(socket),
                                               [](tcp::Session&, std::unique_ptr<message::Message>)
                                               {
                                               }));
        accepted.fetch_add(1, std::memory_order::release);
      });
    }

    std::thread server_thread{[&] { server_io.run(); }};
    std::thread client_thread{[&] { client_io.run(); }};
    while (connected.load(std::memory_order::acquire) < subs ||
      accepted.load(std::memory_order::acquire) < subs)
    {
      std::this_thread::yield();
    }

    // variant 0 = 每会话 encode（旧），variant 1 = 共享帧（新）
    const bool reverse{arg_flag(args, "--reverse")};
    for (int round{0}; round <= 1; ++round)
    {
      const int variant{reverse ? 1 - round : round};
      std::vector<double> enqueue_us, drain_ms;
      std::vector<double> arrivals;
      arrivals.reserve(subs);
      for (std::size_t op{0}; op < ops + warmup; ++op)
      {
        done.store(0, std::memory_order::release);
        for (auto& c : clients)
        {
          c->received = 0;
          c->quota = frame_size;
        }

        std::atomic<bool> enqueued{false};
        Clock::time_point start;
        double enq_us{0};
        asio::post(server_io, [&]
        {
          start = Clock::now();
          if (variant == 0)
          {
            for (auto& s : sessions)
            {
              if (s->async_send(dispatch))
              {
                ++send_errors;
              }
            }
          }
          else
          {
            const auto frame{tcp::encode_frame_shared(dispatch)};
            for (auto& s : sessions)
            {
              if (s->async_send(frame))
              {
                ++send_errors;
              }
            }
          }
          enq_us = std::chrono::duration<double, std::micro>(Clock::now() - start).count();
          enqueued.store(true, std::memory_order::release);
        });
        while (!enqueued.load(std::memory_order::acquire))
        {
          std::this_thread::yield();
        }
        while (done.load(std::memory_order::acquire) < subs)
        {
          std::this_thread::yield();
        }
        if (op < warmup)
        {
          continue;
        }
        enqueue_us.push_back(enq_us);
        double last{0};
        for (auto& c : clients)
        {
          const double ms{std::chrono::duration<double, std::milli>(c->done_at - start).count()};
          arrivals.push_back(ms);
          last = std::max(last, ms);
        }
        drain_ms.push_back(last);
      }
      std::printf("fanout %s: subs=%zu ops=%zu frame=%zuB\n",
                  variant == 0 ? "per-session-encode" : "shared-frame", subs, ops, frame_size);
      std::printf("  enqueue us/op:  p50=%.1f p95=%.1f\n", pct(enqueue_us, 0.5), pct(enqueue_us, 0.95));
      std::printf("  arrival ms:     p50=%.2f p95=%.2f p99=%.2f\n",
                  pct(arrivals, 0.5), pct(arrivals, 0.95), pct(arrivals, 0.99));
      std::printf("  drain ms:       p50=%.2f p95=%.2f\n", pct(drain_ms, 0.5), pct(drain_ms, 0.95));
    }

    std::size_t errors{send_errors.load()};
    for (auto& c : clients)
    {
      errors += c->errors;
    }
    std::printf("  client errors: %zu\n", errors);

    server_io.stop();
    client_io.stop();
    server_thread.join();
    client_thread.join();
    return errors == 0 ? 0 : 1;
  }
}
