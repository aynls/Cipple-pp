#include "benches.hpp"

#include <boost/asio.hpp>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "entity/entity_message.hpp"
#include "protocol/subscription/dispatch_request.hpp"
#include "protocol/subscription/dispatch_response.hpp"
#include "protocol/message.hpp"
#include "protocol/message_type.hpp"
#include "protocol/configuration/put_request.hpp"
#include "protocol/configuration/put_response.hpp"
#include "protocol/subscription/subscribe_request.hpp"
#include "protocol/subscription/subscribe_response.hpp"
#include "tcp/session.hpp"
#include "uuid.hpp"

namespace
{
  namespace asio = boost::asio;
  using Clock = std::chrono::steady_clock;
  using cipple::Uuid;
  using cipple::tcp::Session;

  double pct(std::vector<double>& sorted, double q)
  {
    if (sorted.empty())
    {
      return 0;
    }
    std::sort(sorted.begin(), sorted.end());
    return sorted[std::min<std::size_t>(static_cast<std::size_t>(q * sorted.size()), sorted.size() - 1)];
  }

  // 与 cpp_wire_driver.py 相同的观察边界：t_publish 在发送 PUT 帧前，
  // t_recv 在解析出 DISPATCH 后、回 DISPATCH_RESP 前。
  struct Driver
  {
    asio::io_context io;
    asio::steady_timer pace{io};
    asio::steady_timer deadline{io};
    std::string app{"bench"}, key{"k"};
    std::size_t subs = 1, ops = 20, warmup = 5, value_bytes = 256;
    std::chrono::milliseconds gap{50}, deadline_ms{15000};
    std::optional<std::int64_t> resume_after;

    std::vector<std::shared_ptr<Session>> sub_sessions;
    std::shared_ptr<Session> pub_session;
    std::unordered_map<Uuid, std::size_t> seq_of;
    std::vector<Clock::time_point> t_pub;
    std::vector<double> lats;
    std::vector<std::size_t> per_sub;
    std::size_t ready = 0, errors = 0, seq = 0, responses = 0;
    bool pub_up = false, started = false, finished = false;
    Clock::time_point wall_start, last_pub, last_recv;

    std::size_t total_pairs() const { return subs * ops; }
    std::size_t total_puts() const { return warmup + ops; }

    void connect_all(const asio::ip::tcp::endpoint& endpoint)
    {
      per_sub.assign(subs, 0);
      for (std::size_t i = 0; i < subs; ++i)
      {
        connect(endpoint, [this, i](std::shared_ptr<Session> s)
        {
          sub_sessions.push_back(s);
          if (s->async_send(cipple::message::SubscribeRequest{Uuid::generate(), app, key, resume_after}))
          {
            ++errors;
          }
        });
      }
      connect(endpoint, [this](std::shared_ptr<Session> s)
      {
        pub_session = s;
        pub_up = true;
        maybe_start();
      });
    }

    void connect(const asio::ip::tcp::endpoint& endpoint,
                 std::function<void(std::shared_ptr<Session>)> on_up)
    {
      auto socket = std::make_shared<asio::ip::tcp::socket>(io);
      socket->async_connect(endpoint, [this, socket, on_up = std::move(on_up)](boost::system::error_code ec)
      {
        if (ec)
        {
          ++errors;
          return;
        }
        on_up(Session::start(std::move(*socket), [this](Session& s, std::unique_ptr<cipple::message::Message> m)
                             {
                               on_message(s, *m);
                             },
                             [this](boost::system::error_code)
                             {
                               if (!finished)
                               {
                                 ++errors;
                               }
                             }));
      });
    }

    void on_message(Session& session, const cipple::message::Message& msg)
    {
      using cipple::message::MessageType;
      switch (msg.type())
      {
      case MessageType::kSubscribeResponse:
        {
          const auto& resp = static_cast<const cipple::message::SubscribeResponse&>(msg);
          if (resp.success())
          {
            ++ready;
          }
          else
          {
            ++errors;
          }
          maybe_start();
          break;
        }
      case MessageType::kPutResponse:
        {
          if (!static_cast<const cipple::message::PutResponse&>(msg).success())
          {
            ++errors;
          }
          ++responses;
          break;
        }
      case MessageType::kDispatchRequest:
        {
          const auto& d = static_cast<const cipple::message::DispatchRequest&>(msg);
          last_recv = Clock::now();
          const auto it = seq_of.find(d.entity().uuid());
          if (it != seq_of.end() && it->second >= warmup)
          {
            lats.push_back(std::chrono::duration<double, std::milli>(last_recv - t_pub[it->second]).count());
          }
          if (session.async_send(cipple::message::DispatchResponse{d.uuid(), true}))
          {
            ++errors;
          }
          if (lats.size() >= total_pairs())
          {
            finish();
          }
          break;
        }
      default:
        break;
      }
    }

    void maybe_start()
    {
      if (started || !pub_up || ready < subs)
      {
        return;
      }
      started = true;
      wall_start = Clock::now();
      deadline.expires_after(deadline_ms);
      deadline.async_wait([this](boost::system::error_code ec)
      {
        if (!ec)
        {
          finish();
        }
      });
      send_next();
    }

    void send_next()
    {
      if (finished || seq >= total_puts())
      {
        return;
      }
      const auto id = Uuid::generate();
      seq_of.emplace(id, seq);
      std::string value = "seq " + std::to_string(seq) + " ";
      value.append(value_bytes > value.size() ? value_bytes - value.size() : 0, 'x');
      t_pub.push_back(Clock::now());
      last_pub = Clock::now();
      if (pub_session->async_send(cipple::message::PutRequest{id, app, key, std::move(value)}))
      {
        ++errors;
      }
      ++seq;
      pace.expires_after(gap);
      pace.async_wait([this](boost::system::error_code ec)
      {
        if (!ec)
        {
          send_next();
        }
      });
    }

    void finish()
    {
      if (finished)
      {
        return;
      }
      finished = true;
      const auto wall = std::chrono::duration<double, std::milli>(Clock::now() - wall_start).count();
      const auto drain = std::chrono::duration<double, std::milli>(last_recv - last_pub).count();
      const auto coverage = total_pairs() ? lats.size() * 100.0 / total_pairs() : 0.0;
      std::printf("drive done: ready=%zu/%zu ops=%zu warmup=%zu\n", ready, subs, ops, warmup);
      std::printf("  pairs %zu/%zu coverage=%.1f%%\n", lats.size(), total_pairs(), coverage);
      std::printf("  pair ms:  p50=%.2f p95=%.2f p99=%.2f\n", pct(lats, 0.5), pct(lats, 0.95), pct(lats, 0.99));
      std::printf("  drain=%.0fms wall=%.0fms responses=%zu errors=%zu\n",
                  drain, wall, responses, errors);
      pace.cancel();
      deadline.cancel();
      if (pub_session)
      {
        pub_session->close();
      }
      for (auto& s : sub_sessions)
      {
        s->close();
      }
      pub_session.reset();
      sub_sessions.clear();
    }
  };
}

namespace cipple::bench
{
  int drive(std::span<const char* const> args)
  {
    Driver d;
    const auto server = arg_string(args, "--server");
    if (server.empty())
    {
      std::fprintf(stderr, "usage: cipple_bench drive --server host:port [--app A] [--key K] "
                   "[--subs N] [--ops M] [--warmup W] [--gap-ms G] [--value-bytes B] "
                   "[--resume-after SEQ] [--deadline-ms D]\n");
      return 2;
    }
    const auto colon = server.rfind(':');
    const auto host = server.substr(0, colon);
    const auto port = server.substr(colon + 1);
    d.app = arg_string(args, "--app", "bench");
    d.key = arg_string(args, "--key", "k");
    d.subs = arg_count(args, "--subs", 1);
    d.ops = arg_count(args, "--ops", 20);
    d.warmup = arg_count(args, "--warmup", 5);
    d.value_bytes = arg_count(args, "--value-bytes", 256);
    d.gap = std::chrono::milliseconds{arg_count(args, "--gap-ms", 50)};
    d.deadline_ms = std::chrono::milliseconds{arg_count(args, "--deadline-ms", 15000)};
    if (const auto cursor = arg_string(args, "--resume-after"); !cursor.empty())
    {
      std::int64_t sequence = 0;
      const auto end = cursor.data() + cursor.size();
      const auto [ptr, error] = std::from_chars(cursor.data(), end, sequence);
      if (error != std::errc{} || ptr != end || sequence < 0)
      {
        std::fprintf(stderr, "--resume-after requires a nonnegative sequence\n");
        return 2;
      }
      d.resume_after = sequence;
    }

    asio::ip::tcp::resolver resolver{d.io};
    const auto endpoints = resolver.resolve(host, port);
    d.connect_all(endpoints.begin()->endpoint());
    d.io.run();
    return d.errors == 0 && d.lats.size() == d.total_pairs() ? 0 : 1;
  }
}
