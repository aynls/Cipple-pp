#include <gtest/gtest.h>

#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>

#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "client.hpp"
#include "entity/entity_message.hpp"
#include "raft/raft_node.hpp"
#include "server.hpp"
#include "storage/sqlite_storage.hpp"

namespace
{
  namespace asio = boost::asio;
  using boost::system::error_code;
  using cipple::Uuid;
  using cipple::client::Client;
  using cipple::entity::EntityMessage;
  using cipple::raft::RaftOptions;
  using cipple::server::Server;
  using cipple::storage::AppendResult;
  using cipple::storage::SqliteStorage;

  class SingleServerTest : public testing::Test
  {
  protected:
    void SetUp() override
    {
      ASSERT_TRUE(std::filesystem::create_directory(_directory));
    }

    std::shared_ptr<Server> open_server()
    {
      RaftOptions options;
      options.node_id = 1;
      options.members = {{1, "127.0.0.1:0"}};
      return Server::start(_io, {asio::ip::address_v4::loopback(), 0}, _db, std::move(options));
    }

    void start_server()
    {
      _server = open_server();
    }

    void TearDown() override
    {
      _server.reset();
      std::error_code error;
      std::filesystem::remove_all(_directory, error);
      EXPECT_FALSE(error) << error.message();
    }

    template <typename Predicate>
    bool run_until(Predicate complete)
    {
      if (_io.stopped())
      {
        _io.restart();
      }
      const auto deadline{std::chrono::steady_clock::now() + std::chrono::seconds{3}};
      while (!complete() && _io.run_one_until(deadline) != 0)
      {
        if (std::chrono::steady_clock::now() >= deadline)
        {
          break;
        }
      }
      return complete();
    }

    template <typename Value, typename Send>
    std::expected<Value, error_code> request(Send send)
    {
      using Result = std::expected<Value, error_code>;
      const auto result{std::make_shared<std::optional<Result>>()};
      send([result](const error_code error, Value value)
      {
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
      const auto reply{
        request<std::optional<std::string>>([&](auto done)
        {
          client.get("app", "key", std::move(done));
        })
      };
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

    const std::filesystem::path _directory{
      std::filesystem::temp_directory_path() / ("cipple-raft-test-" + Uuid::generate().to_string())
    };
    const std::filesystem::path _db{_directory / "node.db"};
    asio::io_context _io;
    std::shared_ptr<Server> _server;
  };

  TEST_F(SingleServerTest, WritesReadsAndPushesCommittedChanges)
  {
    start_server();
    std::vector<EntityMessage> events;
    const auto writer{Client::start(_io, _server->local_endpoint())};
    const auto reader{
      Client::start(_io, _server->local_endpoint(), {
                      .on_update = [&](const EntityMessage& event) { events.push_back(event); },
                    })
    };
    ASSERT_TRUE(run_until([&] { return writer->is_connected() && reader->is_connected(); }));
    ASSERT_EQ(request<bool>([&](auto done) { reader->subscribe("app", "key", std::move(done)); }), true);
    ASSERT_TRUE(reads(*reader, std::nullopt));

    const auto uuid{Uuid::generate()};
    const std::string value{"a\0b", 3};
    const auto put{[&](auto done) { writer->put("app", "key", value, std::move(done), uuid); }};
    ASSERT_EQ(request<bool>(put), true);
    ASSERT_TRUE(SqliteStorage{_db}.find_message(uuid));
    ASSERT_TRUE(reads(*reader, value));
    ASSERT_TRUE(run_until([&] { return events.size() == 1; }));
    EXPECT_EQ(events.front().uuid(), uuid);
    EXPECT_EQ(std::get<EntityMessage::Update>(events.front().payload()).value, value);

    ASSERT_EQ(request<bool>(put), true);
    ASSERT_EQ(SqliteStorage{_db}.history("app", "key").size(), 1);
    ASSERT_EQ(request<bool>([&](auto done) { writer->put("other", "key", "ignored", std::move(done)); }), true);
    ASSERT_TRUE(reads(*reader, value));
    EXPECT_EQ(events.size(), 1);

    ASSERT_EQ(request<bool>([&](auto done) { writer->erase("app", "key", std::move(done)); }), true);
    ASSERT_TRUE(reads(*reader, std::nullopt));
    ASSERT_TRUE(run_until([&] { return events.size() == 2; }));
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
    const auto fresh{Client::start(_io, _server->local_endpoint())};
    ASSERT_TRUE(run_until([&] { return fresh->is_connected(); }));
    EXPECT_TRUE(reads(*fresh, std::string{}));
  }

  TEST_F(SingleServerTest, ReadsAnExistingIncrementAndOrdersTheNextWriteAfterIt)
  {
    constexpr std::int64_t future_timestamp{9'000'000'000'000};
    const EntityMessage base{Uuid::generate(), "app", "key", future_timestamp, 2, EntityMessage::Update{"a-x-x-b"}};
    const EntityMessage increment{
      Uuid::generate(), "app", "key", future_timestamp + 1, 2,
      EntityMessage::IncrementalUpdate{base.uuid(), cipple::entity::AtomicOperation::kRemoveEntry, "-x"}
    };
    {
      SqliteStorage storage{_db};
      ASSERT_EQ(storage.append(base, {}), AppendResult::kInserted);
      ASSERT_EQ(storage.append(increment, {}), AppendResult::kInserted);
    }

    start_server();
    const auto client{Client::start(_io, _server->local_endpoint())};
    ASSERT_TRUE(run_until([&] { return client->is_connected(); }));
    ASSERT_TRUE(reads(*client, "a-b"));
    ASSERT_EQ(request<bool>([&](auto done) { client->put("app", "key", "new", std::move(done)); }), true);
    EXPECT_TRUE(reads(*client, "new"));
  }

  TEST_F(SingleServerTest, RejectsConflictingRequestIdsAfterReopen)
  {
    start_server();
    auto client{Client::start(_io, _server->local_endpoint())};
    ASSERT_TRUE(run_until([&] { return client->is_connected(); }));

    const auto uuid{Uuid::generate()};
    const auto put{[&](std::string value)
    {
      return request<bool>([&](auto done) { client->put("app", "key", value, std::move(done), uuid); });
    }};
    ASSERT_EQ(put("original"), true);
    client->stop();
    _server.reset();
    start_server();
    client = Client::start(_io, _server->local_endpoint());
    ASSERT_TRUE(run_until([&] { return client->is_connected(); }));

    EXPECT_EQ(put("original"), true);
    EXPECT_EQ(put("different"), false);
    EXPECT_TRUE(reads(*client, "original"));
    EXPECT_EQ(SqliteStorage{_db}.history("app", "key").size(), 1);
  }

  TEST_F(SingleServerTest, RejectsAnotherServerOpeningTheSameDatabase)
  {
    start_server();
    EXPECT_THROW(open_server(), std::system_error);
  }

  TEST_F(SingleServerTest, RefusesToReinitializeAMissingRaftDatabase)
  {
    start_server();
    const auto client{Client::start(_io, _server->local_endpoint())};
    ASSERT_TRUE(run_until([&] { return client->is_connected(); }));
    ASSERT_EQ(request<bool>([&](auto done) { client->put("app", "key", "kept", std::move(done)); }), true);
    client->stop();
    _server.reset();

    auto raft_db{_db};
    raft_db += ".raft";
    const auto saved{_directory / "saved.raft"};
    std::filesystem::rename(raft_db, saved);
    EXPECT_THROW(open_server(), std::runtime_error);
    std::filesystem::rename(saved, raft_db);
    start_server();
    const auto restored{Client::start(_io, _server->local_endpoint())};
    ASSERT_TRUE(run_until([&] { return restored->is_connected(); }));
    EXPECT_TRUE(reads(*restored, "kept"));
  }
}
