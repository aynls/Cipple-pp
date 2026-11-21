#include <gtest/gtest.h>
#include <sqlite3.h>

#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>

#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "client.hpp"
#include "hlc/hybrid_clock.hpp"
#include "message/dispatch_request.hpp"
#include "message/dispatch_response.hpp"
#include "message/get_request.hpp"
#include "message/get_response.hpp"
#include "message/heartbeat_request.hpp"
#include "message/heartbeat_response.hpp"
#include "message/put_request.hpp"
#include "message/put_response.hpp"
#include "message/subscribe_request.hpp"
#include "message/subscribe_response.hpp"
#include "message/sync_request.hpp"
#include "message/sync_response.hpp"
#include "server.hpp"
#include "storage/sqlite_storage.hpp"
#include "storage/storage_writer.hpp"
#include "tcp/session.hpp"

namespace
{
  namespace asio = boost::asio;
  namespace message = cipple::message;
  namespace storage = cipple::storage;
  namespace tcp = cipple::tcp;
  using boost::system::error_code;
  using cipple::Uuid;
  using cipple::client::Client;
  using cipple::entity::EntityMessage;
  using cipple::server::Server;
  using cipple::server::ServerOptions;
  using cipple::storage::AppendResult;
  using cipple::storage::SqliteStorage;

  class ObservedStorage final : public storage::Storage
  {
  public:
    struct Hooks
    {
      std::function<void(const EntityMessage&)> after_append;
      std::function<void()> before_latest;
      std::function<void(std::string_view, std::int64_t)> before_replay;
    };

    ObservedStorage(const std::filesystem::path& path, Hooks hooks)
      : _store{path}, _hooks{std::move(hooks)}
    {
    }

    AppendResult append(const EntityMessage& version, const std::span<const std::int32_t> targets,
                        const EntityMessage* base) override
    {
      const auto result = _store.append(version, targets, base);
      after_append(result, version);
      return result;
    }

    storage::AppendOutcome append_with_outcome(const EntityMessage& version, const std::span<const std::int32_t> targets,
                                               const EntityMessage* base) override
    {
      const auto outcome = _store.append_with_outcome(version, targets, base);
      after_append(outcome.result, version);
      return outcome;
    }

    std::vector<storage::ItemKey> list_keys() const override { return _store.list_keys(); }

    std::vector<EntityMessage> history(const std::string_view app, const std::string_view key) const override
    {
      return _store.history(app, key);
    }

    std::optional<EntityMessage> latest(const std::string_view app, const std::string_view key) const override
    {
      if (_hooks.before_latest)
      {
        _hooks.before_latest();
      }
      return _store.latest(app, key);
    }

    std::expected<EntityMessage, storage::MessageUnavailable> find_message(const Uuid& uuid) const override
    {
      return _store.find_message(uuid);
    }

    std::vector<storage::PendingId> pending_ids_after(const std::int32_t node,
        const std::optional<storage::PendingCursor>& after, const std::int64_t limit) const override
    {
      return _store.pending_ids_after(node, after, limit);
    }

    std::size_t mark_acked_batch(const std::int32_t node, const std::span<const Uuid> ids) override
    {
      return _store.mark_acked_batch(node, ids);
    }

    std::vector<storage::JournalEntry> journal_after(const std::string_view app, const std::string_view key,
                                                      const std::int64_t after, const std::int64_t limit) const override
    {
      if (_hooks.before_replay)
      {
        _hooks.before_replay(key, after);
      }
      return _store.journal_after(app, key, after, limit);
    }

    std::int64_t journal_sequence(const Uuid& uuid) const override { return _store.journal_sequence(uuid); }
    std::int64_t version_watermark() const override { return _store.version_watermark(); }

    std::size_t recycle(const std::string_view app, const std::string_view key, const std::size_t keep) override
    {
      return _store.recycle(app, key, keep);
    }

  private:
    void after_append(const AppendResult result, const EntityMessage& version) const
    {
      if (_hooks.after_append && result == AppendResult::kInserted)
      {
        _hooks.after_append(version);
      }
    }

    SqliteStorage _store;
    Hooks _hooks;
  };

  struct WireClient
  {
    std::shared_ptr<tcp::Session> session;
    std::shared_ptr<std::vector<std::unique_ptr<message::Message>>> inbox;

    template<typename Message>
    const Message* reply(const Uuid& rpc) const noexcept
    {
      for (const auto& received : *inbox)
      {
        if (received->uuid() == rpc)
        {
          return dynamic_cast<const Message*>(received.get());
        }
      }
      return nullptr;
    }

    std::vector<const message::DispatchRequest*> dispatches() const
    {
      std::vector<const message::DispatchRequest*> updates;
      for (const auto& received : *inbox)
      {
        if (const auto* event = dynamic_cast<const message::DispatchRequest*>(received.get()))
        {
          updates.push_back(event);
        }
      }
      return updates;
    }

    bool send(const message::Message& request) const { return !session->async_send(request); }
  };

  class SingleServerTest : public testing::Test
  {
  protected:
    void start_server(const ServerOptions& options = {}, std::unique_ptr<storage::Storage> writer = {},
                      std::unique_ptr<storage::Storage> reader = {}, storage::StorageWriter::Limits limits = {})
    {
      if (!writer)
      {
        writer = std::make_unique<SqliteStorage>(_db);
      }
      if (!reader)
      {
        reader = std::make_unique<SqliteStorage>(_db);
      }
      _server = Server::start(_io, {asio::ip::address_v4::loopback(), 0},
        std::make_unique<storage::StorageWriter>(std::move(writer), std::move(reader), limits), options);
    }

    WireClient connect_wire()
    {
      asio::ip::tcp::socket socket{_io};
      socket.connect(_server->local_endpoint());
      auto inbox = std::make_shared<std::vector<std::unique_ptr<message::Message>>>();
      auto session = tcp::Session::start(std::move(socket),
        [inbox](tcp::Session&, std::unique_ptr<message::Message> received) {
          inbox->push_back(std::move(received));
        });
      return {std::move(session), std::move(inbox)};
    }

    bool subscribe(const WireClient& client, const std::optional<std::int64_t> cursor = std::nullopt,
                   std::string key = "key")
    {
      const auto rpc = Uuid::generate();
      return client.send(message::SubscribeRequest{rpc, "app", std::move(key), cursor})
          && run_until([&] { return client.reply<message::SubscribeResponse>(rpc); })
          && client.reply<message::SubscribeResponse>(rpc)->success();
    }

    bool heartbeat(const WireClient& client)
    {
      const auto rpc = Uuid::generate();
      return client.send(message::HeartbeatRequest{rpc})
          && run_until([&] { return client.reply<message::HeartbeatResponse>(rpc); });
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

  TEST_F(SingleServerTest, SyncAcknowledgesCommitDespitePostAppendLatestFailure)
  {
    const auto committed = std::make_shared<bool>(false);
    asio::ip::tcp::acceptor offline_peer{_io, {asio::ip::address_v4::loopback(), 0}};
    asio::ip::tcp::acceptor relay_target{_io, {asio::ip::address_v4::loopback(), 0}};
    start_server({.node_id = 1, .peers = {{2, offline_peer.local_endpoint()}, {3, relay_target.local_endpoint()}}},
      std::make_unique<ObservedStorage>(_db, ObservedStorage::Hooks{
        .after_append = [committed](const EntityMessage&) { *committed = true; },
        .before_latest = [committed] {
          if (std::exchange(*committed, false))
          {
            throw std::runtime_error{"injected post-append latest() failure"};
          }
        },
      }));
    const auto watcher = connect_wire();
    ASSERT_TRUE(subscribe(watcher, 0));
    const auto peer = connect_wire();
    const EntityMessage version{Uuid::generate(), "app", "key", 1, 2, EntityMessage::Update{"remote"}};
    const auto sync = [&](const Uuid& rpc) {
      return peer.send(message::SyncRequest{rpc, version, 2})
          && run_until([&] { return peer.reply<message::SyncResponse>(rpc); });
    };
    const auto first = Uuid::generate();
    ASSERT_TRUE(sync(first));
    const SqliteStorage disk{_db};
    const auto sequence = disk.journal_sequence(version.uuid());
    ASSERT_GT(sequence, 0);
    EXPECT_EQ(peer.reply<message::SyncResponse>(first)->result(), message::Result::kSuccess);
    const auto retry = Uuid::generate();
    ASSERT_TRUE(sync(retry));
    EXPECT_EQ(peer.reply<message::SyncResponse>(retry)->result(), message::Result::kDuplicated);
    EXPECT_EQ(disk.journal_after("app", "key", 0, 2).size(), 1);
    const auto pending = disk.pending_ids_after(3, std::nullopt, 2);
    ASSERT_EQ(pending.size(), 1);
    EXPECT_EQ(pending.front().uuid, version.uuid());
    ASSERT_TRUE(run_until([&] { return !watcher.dispatches().empty(); }));
    EXPECT_EQ(watcher.dispatches().front()->entity(), version);
    EXPECT_EQ(watcher.dispatches().front()->sequence(), sequence);
  }

  TEST_F(SingleServerTest, DurableDeliveryUsesNextLogEntryAfterMissedNotification)
  {
    const auto queries = std::make_shared<int>(0);
    start_server({}, {}, std::make_unique<ObservedStorage>(_db, ObservedStorage::Hooks{
      .before_replay = [queries](std::string_view, std::int64_t) { ++*queries; },
    }));
    const auto watcher = connect_wire();
    ASSERT_TRUE(subscribe(watcher, 0));
    ASSERT_TRUE(run_until([&] { return *queries == 1; }));
    const auto cohort = connect_wire();
    ASSERT_TRUE(subscribe(cohort, 0));
    ASSERT_TRUE(run_until([&] { return *queries == 2; }));
    SqliteStorage disk{_db};
    const EntityMessage missed{Uuid::generate(), "app", "key", 1, 2, EntityMessage::Update{"missed"}};
    const EntityMessage unrelated{Uuid::generate(), "app", "other", 2, 2, EntityMessage::Delete{}};
    ASSERT_EQ(disk.append(missed, {}), AppendResult::kInserted);
    ASSERT_EQ(disk.append(unrelated, {}), AppendResult::kInserted);

    const auto publisher = connect_wire();
    const auto newer = Uuid::generate();
    ASSERT_TRUE(publisher.send(message::PutRequest{newer, "app", "key", "newer"}));
    ASSERT_TRUE(run_until([&] {
      return publisher.reply<message::PutResponse>(newer) && !watcher.dispatches().empty() && !cohort.dispatches().empty();
    }));
    ASSERT_TRUE(publisher.reply<message::PutResponse>(newer)->success());
    const auto* first = watcher.dispatches().front();
    ASSERT_EQ(first->entity().uuid(), missed.uuid());
    EXPECT_EQ(cohort.dispatches().front()->uuid(), first->uuid());
    EXPECT_EQ(*queries, 3);
    ASSERT_TRUE(watcher.send(message::DispatchResponse{Uuid::generate(), true}));
    ASSERT_TRUE(heartbeat(watcher));
    EXPECT_EQ(watcher.dispatches().size(), 1);
    watcher.session->close();

    const auto resumed = connect_wire();
    ASSERT_TRUE(subscribe(resumed, 0));
    ASSERT_TRUE(run_until([&] { return !resumed.dispatches().empty(); }));
    const auto* replayed = resumed.dispatches().front();
    EXPECT_EQ(replayed->sequence(), first->sequence());
    EXPECT_EQ(replayed->entity(), missed);
    EXPECT_NE(replayed->uuid(), first->uuid());

    // Replacing the subscription on the same session invalidates its old ACK.
    ASSERT_TRUE(subscribe(resumed, 0));
    ASSERT_TRUE(run_until([&] { return resumed.dispatches().size() == 2; }));
    const auto* replacement = resumed.dispatches().back();
    EXPECT_EQ(replacement->entity(), missed);
    EXPECT_NE(replacement->uuid(), replayed->uuid());
    ASSERT_TRUE(resumed.send(message::DispatchResponse{replayed->uuid(), true}));
    ASSERT_TRUE(heartbeat(resumed));
    EXPECT_EQ(resumed.dispatches().size(), 2);
    ASSERT_TRUE(resumed.send(message::DispatchResponse{replacement->uuid(), true}));
    ASSERT_TRUE(run_until([&] { return resumed.dispatches().size() == 3; }));
    EXPECT_EQ(resumed.dispatches().back()->entity().uuid(), newer);
    EXPECT_GT(resumed.dispatches().back()->sequence(), disk.journal_sequence(unrelated.uuid()));
  }

  TEST_F(SingleServerTest, ReplayReadFailureLeavesCommitAndOtherSubscriptionsIntact)
  {
    const auto fail_next = std::make_shared<bool>(false);
    const auto initial_reads = std::make_shared<int>(0);
    start_server({}, {}, std::make_unique<ObservedStorage>(_db, ObservedStorage::Hooks{
      .before_replay = [fail_next, initial_reads](const std::string_view key, std::int64_t) {
        ++*initial_reads;
        if (key == "key" && std::exchange(*fail_next, false))
        {
          throw std::runtime_error{"injected subscription journal read failure"};
        }
      },
    }));
    const auto affected = connect_wire();
    const auto unaffected = connect_wire();
    ASSERT_TRUE(subscribe(affected, 0));
    ASSERT_TRUE(subscribe(unaffected, 0, "other"));
    ASSERT_TRUE(run_until([&] { return *initial_reads == 2; }));

    *fail_next = true;
    const auto publisher = connect_wire();
    const auto failed_delivery = Uuid::generate();
    const auto other_update = Uuid::generate();
    ASSERT_TRUE(publisher.send(message::PutRequest{failed_delivery, "app", "key", "committed"}));
    ASSERT_TRUE(publisher.send(message::PutRequest{other_update, "app", "other", "delivered"}));
    ASSERT_TRUE(run_until([&] {
      return publisher.reply<message::PutResponse>(failed_delivery) && publisher.reply<message::PutResponse>(other_update)
          && !affected.session->is_open() && !unaffected.dispatches().empty();
    }));
    EXPECT_TRUE(publisher.reply<message::PutResponse>(failed_delivery)->success());
    EXPECT_TRUE(publisher.reply<message::PutResponse>(other_update)->success());
    EXPECT_TRUE(affected.dispatches().empty());
    EXPECT_EQ(unaffected.dispatches().front()->entity().uuid(), other_update);
    const SqliteStorage disk{_db};
    EXPECT_TRUE(disk.find_message(failed_delivery));

    const auto resumed = connect_wire();
    ASSERT_TRUE(subscribe(resumed, 0));
    ASSERT_TRUE(run_until([&] { return !resumed.dispatches().empty(); }));
    EXPECT_EQ(resumed.dispatches().front()->entity().uuid(), failed_delivery);
  }

  TEST_F(SingleServerTest, ConcurrentLocalWriteTicksAfterQueuedRemoteVersion)
  {
    asio::ip::tcp::acceptor peer_endpoint{_io, {asio::ip::address_v4::loopback(), 0}};
    std::promise<void> unblock;
    const auto gate = unblock.get_future().share();
    const auto remote_id = Uuid::generate();
    start_server({.node_id = 1, .peers = {{2, peer_endpoint.local_endpoint()}}},
      std::make_unique<ObservedStorage>(_db, ObservedStorage::Hooks{
        .after_append = [gate, remote_id](const EntityMessage& version) {
          if (version.uuid() == remote_id)
          {
            gate.wait();
          }
        },
      }));
    const auto watcher = connect_wire();
    ASSERT_TRUE(subscribe(watcher));
    const auto peer = connect_wire();
    constexpr auto high_hlc = (std::int64_t{1} << 62) + 9'000'000'000'000LL * 65'536;
    constexpr auto rejected_hlc = high_hlc + 100;
    const EntityMessage rejected{Uuid::generate(), "app", "reject", rejected_hlc, 2, EntityMessage::Delete{}};
    ASSERT_NO_FATAL_FAILURE(execute_sql(R"sql(
      CREATE TRIGGER reject_remote BEFORE INSERT ON message
      WHEN NEW.key = CAST('reject' AS BLOB)
      BEGIN SELECT RAISE(ABORT, 'test rejects remote sync'); END;
    )sql"));
    const auto rejected_rpc = Uuid::generate();
    ASSERT_TRUE(peer.send(message::SyncRequest{rejected_rpc, rejected, 2}));
    ASSERT_TRUE(run_until([&] { return peer.reply<message::SyncResponse>(rejected_rpc); }));
    EXPECT_EQ(peer.reply<message::SyncResponse>(rejected_rpc)->result(), message::Result::kRejected);

    const EntityMessage remote{remote_id, "app", "key", high_hlc, 2, EntityMessage::Delete{}};
    const auto sync_id = Uuid::generate();
    const auto local_id = Uuid::generate();
    ASSERT_TRUE(peer.send(message::SyncRequest{sync_id, remote, 2}));
    ASSERT_TRUE(peer.send(message::PutRequest{local_id, "app", "key", "local"}));
    // This response proves both mutations were enqueued before the remote completion could run.
    ASSERT_TRUE(heartbeat(peer));
    unblock.set_value();
    ASSERT_TRUE(run_until([&] {
      return peer.reply<message::SyncResponse>(sync_id) && peer.reply<message::PutResponse>(local_id)
          && watcher.dispatches().size() == 2;
    }));
    EXPECT_EQ(peer.reply<message::SyncResponse>(sync_id)->result(), message::Result::kSuccess);
    EXPECT_TRUE(peer.reply<message::PutResponse>(local_id)->success());
    const auto* live = watcher.dispatches().back();
    EXPECT_EQ(live->entity().uuid(), local_id);
    EXPECT_GT(live->entity().last_update(), high_hlc);
    EXPECT_LT(live->entity().last_update(), rejected_hlc);
    const auto get_id = Uuid::generate();
    ASSERT_TRUE(peer.send(message::GetRequest{get_id, "app", "key"}));
    ASSERT_TRUE(run_until([&] { return peer.reply<message::GetResponse>(get_id); }));
    const auto& versions = peer.reply<message::GetResponse>(get_id)->items();
    ASSERT_EQ(versions.size(), 1);
    EXPECT_EQ(versions.back().uuid(), live->entity().uuid());
  }

  TEST_F(SingleServerTest, RejectsWritesAndSyncsOverTheStorageByteBudget)
  {
    asio::ip::tcp::acceptor offline_peer{_io, {asio::ip::address_v4::loopback(), 0}};
    start_server({.node_id = 1, .peers = {{2, offline_peer.local_endpoint()}}}, {}, {}, {.max_request_bytes = 512});
    const auto client = connect_wire();
    const auto put_id = Uuid::generate();
    const auto sync_id = Uuid::generate();
    const std::string large(2048, 'v');
    const EntityMessage version{Uuid::generate(), "app", "key", 100, 2, EntityMessage::Update{large}};
    ASSERT_TRUE(client.send(message::PutRequest{put_id, "app", "key", large}));
    ASSERT_TRUE(client.send(message::SyncRequest{sync_id, version, 2}));
    ASSERT_TRUE(run_until([&] {
      return client.reply<message::PutResponse>(put_id) && client.reply<message::SyncResponse>(sync_id);
    }));
    EXPECT_FALSE(client.reply<message::PutResponse>(put_id)->success());
    EXPECT_EQ(client.reply<message::SyncResponse>(sync_id)->result(), message::Result::kRejected);
    const SqliteStorage disk{_db};
    EXPECT_FALSE(disk.find_message(put_id));
    EXPECT_FALSE(disk.find_message(version.uuid()));

    const auto small_id = Uuid::generate();
    ASSERT_TRUE(client.send(message::PutRequest{small_id, "app", "key", "small"}));
    ASSERT_TRUE(run_until([&] { return client.reply<message::PutResponse>(small_id); }));
    EXPECT_TRUE(client.reply<message::PutResponse>(small_id)->success());
    EXPECT_TRUE(disk.find_message(small_id));
  }

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
