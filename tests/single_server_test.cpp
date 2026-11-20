#include <gtest/gtest.h>
#include <sqlite3.h>

#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>

#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "client.hpp"
#include "hlc/hybrid_clock.hpp"
#include "server.hpp"
#include "storage/sqlite_storage.hpp"

namespace
{
  namespace asio = boost::asio;
  using boost::system::error_code;
  using cipple::Uuid;
  using cipple::client::Client;
  using cipple::entity::EntityMessage;
  using cipple::server::Server;
  using cipple::storage::AppendResult;
  using cipple::storage::SqliteStorage;

  class SingleServerTest : public testing::Test
  {
  protected:
    void start_server()
    {
      _server = Server::start(_io, {asio::ip::address_v4::loopback(), 0}, std::make_unique<SqliteStorage>(_db));
    }

    void TearDown() override
    {
      _server.reset();
      std::error_code error;
      std::filesystem::remove(_db, error);
      EXPECT_FALSE(error) << error.message();
    }

    template<typename Predicate>
    bool run_until(Predicate complete)
    {
      if (_io.stopped())
      {
        _io.restart();
      }
      const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{3};
      while (!complete() && _io.run_one_until(deadline) != 0)
      {
        if (std::chrono::steady_clock::now() >= deadline)
        {
          break;
        }
      }
      return complete();
    }

    template<typename Value, typename Send>
    std::expected<Value, error_code> request(Send send)
    {
      using Result = std::expected<Value, error_code>;
      const auto result = std::make_shared<std::optional<Result>>();
      send([result](const error_code error, Value value) {
        if (error)
        {
          result->emplace(std::unexpected{error});
        }
        else
        {
          result->emplace(std::move(value));
        }
      });
      if (!run_until([&] { return result->has_value(); }))
      {
        return std::unexpected{error_code{asio::error::timed_out}};
      }
      return std::move(**result);
    }

    testing::AssertionResult reads(Client& client, const std::optional<std::string>& expected)
    {
      const auto reply = request<std::optional<std::string>>([&](auto done) {
        client.get("app", "key", std::move(done));
      });
      if (!reply)
      {
        return testing::AssertionFailure() << reply.error().message();
      }
      if (*reply == expected)
      {
        return testing::AssertionSuccess();
      }
      return testing::AssertionFailure() << "expected " << testing::PrintToString(expected)
                                         << ", got " << testing::PrintToString(*reply);
    }

    void execute_sql(const char* sql) const
    {
      const auto path = _db.u8string();
      sqlite3* raw_db = nullptr;
      const auto status = sqlite3_open_v2(reinterpret_cast<const char*>(path.c_str()), &raw_db,
                                         SQLITE_OPEN_READWRITE, nullptr);
      const std::unique_ptr<sqlite3, decltype(&sqlite3_close)> db{raw_db, sqlite3_close};
      ASSERT_EQ(status, SQLITE_OK);
      ASSERT_EQ(sqlite3_exec(db.get(), sql, nullptr, nullptr, nullptr), SQLITE_OK) << sqlite3_errmsg(db.get());
    }

    const std::filesystem::path _db =
        std::filesystem::temp_directory_path() / ("cipple-single-server-" + Uuid::generate().to_string() + ".db");
    asio::io_context _io;
    std::shared_ptr<Server> _server;
  };

  TEST_F(SingleServerTest, WritesReadsAndPushesCommittedChanges)
  {
    start_server();
    std::vector<EntityMessage> events;
    const auto writer = Client::start(_io, _server->local_endpoint());
    const auto reader = Client::start(_io, _server->local_endpoint(), {
      .on_update = [&](const EntityMessage& event) { events.push_back(event); },
    });
    ASSERT_TRUE(run_until([&] { return writer->is_connected() && reader->is_connected(); }));
    ASSERT_EQ(request<bool>([&](auto done) { reader->subscribe("app", "key", std::move(done)); }), true);
    ASSERT_TRUE(reads(*reader, std::nullopt));

    const auto uuid = Uuid::generate();
    const std::string value{"a\0b", 3};
    const auto put = [&](auto done) { writer->put("app", "key", value, std::move(done), uuid); };
    ASSERT_EQ(request<bool>(put), true);
    ASSERT_TRUE(SqliteStorage{_db}.find_message(uuid));
    ASSERT_TRUE(reads(*reader, value));
    ASSERT_EQ(events.size(), 1);
    EXPECT_EQ(events.front().uuid(), uuid);
    EXPECT_EQ(std::get<EntityMessage::Update>(events.front().payload()).value, value);

    ASSERT_EQ(request<bool>(put), true);
    ASSERT_EQ(SqliteStorage{_db}.history("app", "key").size(), 1);
    ASSERT_EQ(request<bool>([&](auto done) { writer->put("other", "key", "ignored", std::move(done)); }), true);
    ASSERT_TRUE(reads(*reader, value));
    EXPECT_EQ(events.size(), 1);

    ASSERT_EQ(request<bool>([&](auto done) { writer->erase("app", "key", std::move(done)); }), true);
    ASSERT_TRUE(reads(*reader, std::nullopt));
    ASSERT_EQ(events.size(), 2);
    EXPECT_EQ(events.back().operation_type(), cipple::entity::OperationType::kDelete);

    ASSERT_EQ(request<bool>([&](auto done) { reader->unsubscribe("app", "key", std::move(done)); }), true);
    ASSERT_EQ(request<bool>([&](auto done) { writer->put("app", "key", "", std::move(done)); }), true);
    ASSERT_TRUE(reads(*reader, std::string{}));
    EXPECT_EQ(events.size(), 2);

    // Reopen the file in a new server and query through a fresh connection.
    reader->stop();
    writer->stop();
    _server.reset();
    start_server();
    const auto fresh = Client::start(_io, _server->local_endpoint());
    ASSERT_TRUE(run_until([&] { return fresh->is_connected(); }));
    EXPECT_TRUE(reads(*fresh, std::string{}));
  }

  TEST_F(SingleServerTest, FailedWriteDoesNotPublishAndCanBeRetried)
  {
    start_server();
    ASSERT_NO_FATAL_FAILURE(execute_sql(R"sql(
      CREATE TRIGGER reject_update BEFORE INSERT ON message
      BEGIN SELECT RAISE(ABORT, 'test rejects update'); END;
    )sql"));
    std::vector<Uuid> delivered;
    const auto writer = Client::start(_io, _server->local_endpoint());
    const auto reader = Client::start(_io, _server->local_endpoint(), {
      .on_update = [&](const EntityMessage& version) { delivered.push_back(version.uuid()); },
    });
    ASSERT_TRUE(run_until([&] { return writer->is_connected() && reader->is_connected(); }));
    ASSERT_EQ(request<bool>([&](auto done) { reader->subscribe("app", "key", std::move(done)); }), true);

    const auto uuid = Uuid::generate();
    const auto put = [&](auto done) { writer->put("app", "key", "value", std::move(done), uuid); };
    ASSERT_EQ(request<bool>(put), false);
    ASSERT_TRUE(reads(*reader, std::nullopt));
    EXPECT_TRUE(delivered.empty());
    const SqliteStorage committed{_db};
    EXPECT_TRUE(committed.list_keys().empty());

    ASSERT_NO_FATAL_FAILURE(execute_sql("DROP TRIGGER reject_update"));
    ASSERT_EQ(request<bool>(put), true);
    EXPECT_TRUE(reads(*reader, "value"));
    EXPECT_EQ(delivered, std::vector{uuid});
  }

  TEST_F(SingleServerTest, ReadsAnExistingIncrementAndOrdersTheNextWriteAfterIt)
  {
    constexpr std::int64_t future_timestamp = 9'000'000'000'000;
    const EntityMessage base{Uuid::generate(), "app", "key", future_timestamp, 2, EntityMessage::Update{"a-x-x-b"}};
    const EntityMessage increment{Uuid::generate(), "app", "key", future_timestamp + 1, 2,
      EntityMessage::IncrementalUpdate{base.uuid(), cipple::entity::AtomicOperation::kRemoveEntry, "-x"}};
    {
      SqliteStorage storage{_db};
      ASSERT_EQ(storage.append(base, {}), AppendResult::kInserted);
      ASSERT_EQ(storage.append(increment, {}), AppendResult::kInserted);
    }

    start_server();
    const auto client = Client::start(_io, _server->local_endpoint());
    ASSERT_TRUE(run_until([&] { return client->is_connected(); }));
    ASSERT_TRUE(reads(*client, "a-b"));
    ASSERT_EQ(request<bool>([&](auto done) { client->put("app", "key", "new", std::move(done)); }), true);
    EXPECT_TRUE(reads(*client, "new"));
  }

  TEST(HybridClockTest, CausalWriteFollowsObservedFastClockAcrossReopen)
  {
    cipple::hlc::HybridClock fast{60'000};
    cipple::hlc::HybridClock slow{-60'000};
    const auto remote = fast.tick();
    slow.observe(remote);
    const auto after_sync = slow.tick();
    ASSERT_GT(after_sync, remote);
    cipple::hlc::HybridClock restarted{-60'000};
    restarted.observe(after_sync);
    EXPECT_GT(restarted.tick(), after_sync);
  }
}
