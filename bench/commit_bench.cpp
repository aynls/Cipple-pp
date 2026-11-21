#include "benches.hpp"

#include <boost/asio.hpp>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <vector>

#include "entity/entity_message.hpp"
#include "storage/sqlite_storage.hpp"
#include "storage/storage_writer.hpp"
#include "uuid.hpp"

namespace
{
  namespace asio = boost::asio;
  using Clock = std::chrono::steady_clock;

  double pct(std::vector<double>& sorted, double q)
  {
    if (sorted.empty())
    {
      return 0;
    }
    std::sort(sorted.begin(), sorted.end());
    return sorted[std::min<std::size_t>(static_cast<std::size_t>(q * sorted.size()), sorted.size() - 1)];
  }

  double us_since(Clock::time_point t0)
  {
    return std::chrono::duration<double, std::micro>(Clock::now() - t0).count();
  }
}

namespace cipple::bench
{
  // 事件循环停顿对比：同一 io_context 上，每次提交前 post 一个探针任务，
  // 探针的排队延迟即“commit 是否阻塞了循环”的直接度量。
  // inline：在 I/O 线程上同步 append（旧）；writer：StorageWriter 线程提交（新）。
  int commit(std::span<const char* const> args)
  {
    const auto ops = arg_count(args, "--ops", 300);
    const auto db = std::filesystem::temp_directory_path() /
      ("cipple-commit-bench-" + Uuid::generate().to_string() + ".db");

    for (int variant = 0; variant <= 1; ++variant)
    {
      asio::io_context io;
      std::vector<entity::EntityMessage> messages;
      messages.reserve(ops);
      for (std::size_t i = 0; i < ops; ++i)
      {
        messages.emplace_back(Uuid::generate(), "bench-app", "bench-key",
                              1'700'000'000'000 + static_cast<std::int64_t>(i), 1,
                              entity::EntityMessage::Update{"v"});
      }
      std::vector<double> probes, commits;
      probes.reserve(ops);
      commits.reserve(ops);
      std::size_t op = 0;

      if (variant == 0)
      {
        storage::SqliteStorage storage{db};
        std::function < void() > step;
        step = [&]
        {
          if (op >= ops)
          {
            return;
          }
          const auto posted = Clock::now();
          asio::post(io, [&, posted] { probes.push_back(us_since(posted)); });
          const auto t0 = Clock::now();
          storage.append(messages[op], {});
          commits.push_back(us_since(t0));
          ++op;
          asio::post(io, step);
        };
        asio::post(io, step);
        io.run();
      }
      else
      {
        storage::StorageWriter writer{
          std::make_unique<storage::SqliteStorage>(db),
          std::make_unique<storage::SqliteStorage>(db)
        };
        std::size_t done_count = 0;
        auto work = asio::make_work_guard(io);
        std::function < void() > step;
        step = [&]
        {
          if (op >= ops)
          {
            return;
          }
          const auto posted = Clock::now();
          asio::post(io, [&, posted] { probes.push_back(us_since(posted)); });
          const auto t0 = Clock::now();
          const auto i = op++;
          if (!writer.submit(io, [&, i](storage::Storage& s) { s.append(messages[i], {}); },
                             [&, t0]
                             {
                               commits.push_back(us_since(t0));
                               if (++done_count == ops)
                               {
                                 work.reset();
                               }
                               else
                               {
                                 asio::post(io, step);
                               }
                             }))
          {
            std::fprintf(stderr, "submit returned false at op %zu\n", i);
          }
        };
        asio::post(io, step);
        io.run();
        writer.stop();
      }

      std::printf("commit %s: ops=%zu\n", variant == 0 ? "inline-on-loop" : "writer-thread", ops);
      std::printf("  io probe delay us: p50=%8.1f p95=%8.1f p99=%8.1f\n",
                  pct(probes, 0.5), pct(probes, 0.95), pct(probes, 0.99));
      std::printf("  commit latency us: p50=%8.1f p95=%8.1f p99=%8.1f\n",
                  pct(commits, 0.5), pct(commits, 0.95), pct(commits, 0.99));
    }

    std::error_code ec;
    std::filesystem::remove(db, ec);
    std::filesystem::remove(db.string() + "-wal", ec);
    std::filesystem::remove(db.string() + "-shm", ec);
    return 0;
  }
}
