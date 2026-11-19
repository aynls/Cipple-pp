#include <gtest/gtest.h>
#include <sqlite3.h>

#include <array>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "storage/sqlite_storage.hpp"

namespace
{
  using cipple::Uuid;
  using cipple::entity::AtomicOperation;
  using cipple::entity::EntityMessage;
  using cipple::storage::AppendResult;
  using cipple::storage::MessageUnavailable;
  using cipple::storage::SqliteStorage;

  EntityMessage update(const std::int64_t version, std::string value)
  {
    return {Uuid::generate(), "app", "key", version, 1, EntityMessage::Update{std::move(value)}};
  }

  EntityMessage increment(const EntityMessage& base, const std::int64_t version)
  {
    return {Uuid::generate(), "app", "key", version, 1,
            EntityMessage::IncrementalUpdate{base.uuid(), AtomicOperation::kRemoveEntry, "removed"}};
  }

  Uuid ordered_uuid(const std::uint8_t last_byte)
  {
    std::array<std::uint8_t, 16> bytes{};
    bytes.back() = last_byte;
    return Uuid::from_bytes(bytes);
  }

  class SqliteStorageTest : public testing::Test
  {
  protected:
    void TearDown() override
    {
      std::error_code error;
      std::filesystem::remove(_path, error);
      EXPECT_FALSE(error) << error.message();
    }

    void execute_sql(const char* sql) const
    {
      const auto path = _path.u8string();
      sqlite3* raw_db = nullptr;
      const auto open_status = sqlite3_open_v2(reinterpret_cast<const char*>(path.c_str()), &raw_db,
                                              SQLITE_OPEN_READWRITE, nullptr);
      const std::unique_ptr<sqlite3, decltype(&sqlite3_close)> db{raw_db, sqlite3_close};
      ASSERT_EQ(open_status, SQLITE_OK);
      ASSERT_EQ(sqlite3_exec(db.get(), sql, nullptr, nullptr, nullptr), SQLITE_OK) << sqlite3_errmsg(db.get());
    }

    const std::filesystem::path _path =
        std::filesystem::temp_directory_path() / ("cipple-storage-" + Uuid::generate().to_string() + ".db");
  };

  TEST_F(SqliteStorageTest, RestoresHistoryAndPendingTargetsAfterReopen)
  {
    const auto full = update(1, std::string{"a\0b", 3});
    const auto delta = increment(full, 2);
    const EntityMessage other{Uuid::generate(), "app", "other", 3, -2, EntityMessage::Update{""}};
    const std::array<std::int32_t, 3> targets{7, 8, 8};
    {
      SqliteStorage storage{_path};
      ASSERT_EQ(storage.append(full, {}), AppendResult::kInserted);
      ASSERT_EQ(storage.append(delta, targets), AppendResult::kInserted);
      ASSERT_EQ(storage.append(other, {}), AppendResult::kInserted);
      EXPECT_FALSE(storage.mark_acked(delta.uuid(), 9));
      EXPECT_TRUE(storage.mark_acked(delta.uuid(), 7));
      EXPECT_FALSE(storage.mark_acked(delta.uuid(), 7));
    }

    const SqliteStorage storage{_path};
    const auto keys = storage.list_keys();
    ASSERT_EQ(keys.size(), 2);
    EXPECT_EQ(keys[0].application_name, "app");
    EXPECT_EQ(keys[0].key, "key");
    EXPECT_EQ(keys[1].key, "other");
    const auto messages = storage.history("app", "key");
    ASSERT_EQ(messages.size(), 2);
    EXPECT_EQ(messages[0].uuid(), full.uuid());
    EXPECT_EQ(std::get<EntityMessage::Update>(messages[0].payload()).value, (std::string{"a\0b", 3}));
    EXPECT_EQ(messages[1].uuid(), delta.uuid());
    const auto& stored_delta = std::get<EntityMessage::IncrementalUpdate>(messages[1].payload());
    EXPECT_EQ(stored_delta.base_message_uuid, full.uuid());
    EXPECT_EQ(stored_delta.atomic_operation, AtomicOperation::kRemoveEntry);
    EXPECT_EQ(stored_delta.value, "removed");
    const auto empty = storage.find_message(other.uuid());
    ASSERT_TRUE(empty);
    EXPECT_EQ(empty->last_update_server_id(), -2);
    EXPECT_TRUE(std::get<EntityMessage::Update>(empty->payload()).value.empty());
    EXPECT_TRUE(storage.pending_for_node(7).empty());
    const auto pending = storage.pending_for_node(8);
    ASSERT_EQ(pending.size(), 1);
    EXPECT_EQ(pending[0].uuid(), delta.uuid());
  }

  TEST_F(SqliteStorageTest, RetryPreservesTheFirstVersionAndAckProgress)
  {
    SqliteStorage storage{_path};
    const auto full = update(1, "first");
    const std::array<std::int32_t, 1> original_targets{7};
    ASSERT_EQ(storage.append(full, original_targets), AppendResult::kInserted);
    ASSERT_TRUE(storage.mark_acked(full.uuid(), 7));

    const EntityMessage retry{full.uuid(), "app", "key", 20, 2, EntityMessage::Update{"retry"}};
    const std::array<std::int32_t, 2> retry_targets{7, 8};
    EXPECT_EQ(storage.append(retry, retry_targets), AppendResult::kAlreadyExists);
    const auto stored = storage.find_message(full.uuid());
    ASSERT_TRUE(stored);
    EXPECT_EQ(stored->last_update(), 1);
    EXPECT_EQ(stored->last_update_server_id(), 1);
    EXPECT_EQ(std::get<EntityMessage::Update>(stored->payload()).value, "first");
    EXPECT_TRUE(storage.pending_for_node(7).empty());
    EXPECT_TRUE(storage.pending_for_node(8).empty());
  }

  TEST_F(SqliteStorageTest, RollsBackItemMessageAndFirstAckWhenLaterAckFails)
  {
    SqliteStorage storage{_path};
    ASSERT_NO_FATAL_FAILURE(execute_sql(R"sql(
      CREATE TRIGGER reject_second_target BEFORE INSERT ON ack
      WHEN NEW.node_id = 22 AND EXISTS (
        SELECT 1 FROM ack WHERE message_uuid = NEW.message_uuid AND node_id = 21
      )
      BEGIN
        SELECT RAISE(ABORT, 'test failed second ACK insert');
      END;
    )sql"));
    const auto full = update(1, "value");
    const std::array<std::int32_t, 2> targets{21, 22};
    EXPECT_THROW(storage.append(full, targets), std::runtime_error);

    const SqliteStorage reopened{_path};
    EXPECT_TRUE(reopened.list_keys().empty());
    const auto lookup = reopened.find_message(full.uuid());
    ASSERT_FALSE(lookup);
    EXPECT_EQ(lookup.error(), MessageUnavailable::kMissing);
    EXPECT_TRUE(reopened.pending_for_node(21).empty());
    EXPECT_TRUE(reopened.pending_for_node(22).empty());
    EXPECT_EQ(storage.append(full, {}), AppendResult::kInserted);
  }

  TEST_F(SqliteStorageTest, RollsBackAppendWhenCommitFails)
  {
    SqliteStorage storage{_path};
    ASSERT_NO_FATAL_FAILURE(execute_sql(R"sql(
      CREATE TABLE test_commit_guard (
        missing_uuid BLOB REFERENCES message(uuid) DEFERRABLE INITIALLY DEFERRED
      );
      CREATE TRIGGER reject_ack_at_commit AFTER INSERT ON ack
      BEGIN
        INSERT INTO test_commit_guard VALUES (zeroblob(16));
      END;
    )sql"));
    const auto full = update(1, "value");
    const std::array<std::int32_t, 1> targets{7};

    ASSERT_THROW(storage.append(full, targets), std::runtime_error);
    EXPECT_TRUE(storage.list_keys().empty());
    const auto lookup = storage.find_message(full.uuid());
    ASSERT_FALSE(lookup);
    EXPECT_EQ(lookup.error(), MessageUnavailable::kMissing);
    EXPECT_TRUE(storage.pending_for_node(7).empty());

    ASSERT_NO_FATAL_FAILURE(execute_sql("DROP TRIGGER reject_ack_at_commit"));
    ASSERT_EQ(storage.append(full, targets), AppendResult::kInserted);
    const auto pending = storage.pending_for_node(7);
    ASSERT_EQ(pending.size(), 1);
    EXPECT_EQ(pending[0].uuid(), full.uuid());
  }

  TEST_F(SqliteStorageTest, KeepsPendingIncrementAndItsBaseUntilAllTargetsAck)
  {
    SqliteStorage storage{_path};
    const auto full = update(1, "base");
    const auto delta = increment(full, 2);
    const auto newest = update(3, "newest");
    const std::array<std::int32_t, 2> targets{7, 8};
    ASSERT_EQ(storage.append(full, {}), AppendResult::kInserted);
    ASSERT_EQ(storage.append(delta, targets), AppendResult::kInserted);
    ASSERT_EQ(storage.append(newest, {}), AppendResult::kInserted);

    EXPECT_EQ(storage.recycle("app", "key", 1), 0);
    ASSERT_TRUE(storage.mark_acked(delta.uuid(), 7));
    EXPECT_EQ(storage.recycle("app", "key", 1), 0);
    ASSERT_TRUE(storage.find_message(full.uuid()));
    ASSERT_TRUE(storage.mark_acked(delta.uuid(), 8));
    EXPECT_EQ(storage.recycle("app", "key", 1), 2);

    const auto history = storage.history("app", "key");
    ASSERT_EQ(history.size(), 1);
    EXPECT_EQ(history[0].uuid(), newest.uuid());
    const auto recycled_delta = storage.find_message(delta.uuid());
    ASSERT_FALSE(recycled_delta);
    EXPECT_EQ(recycled_delta.error(), MessageUnavailable::kRecycled);
    EXPECT_EQ(storage.append(delta, targets), AppendResult::kAlreadyExists);
    EXPECT_FALSE(storage.mark_acked(delta.uuid(), 8));
    EXPECT_TRUE(storage.pending_for_node(8).empty());
    EXPECT_EQ(storage.recycle("app", "key", 1), 0);
  }

  TEST_F(SqliteStorageTest, KeepsDeleteAndUsesTheSameOrderingForHistoryAndRecycling)
  {
    SqliteStorage storage{_path};
    const EntityMessage earlier{ordered_uuid(9), "app", "key", 1, 9, EntityMessage::Update{"earlier"}};
    const EntityMessage lower_server{ordered_uuid(8), "app", "key", 2, -1, EntityMessage::Update{"lower server"}};
    const EntityMessage lower_uuid{ordered_uuid(1), "app", "key", 2, 1, EntityMessage::Update{"lower uuid"}};
    const EntityMessage deletion{ordered_uuid(2), "app", "key", 2, 1, EntityMessage::Delete{}};
    const EntityMessage other{ordered_uuid(3), "other", "key", 2, 1, EntityMessage::Update{"other app"}};
    ASSERT_EQ(storage.append(deletion, {}), AppendResult::kInserted);
    ASSERT_EQ(storage.append(lower_uuid, {}), AppendResult::kInserted);
    ASSERT_EQ(storage.append(lower_server, {}), AppendResult::kInserted);
    ASSERT_EQ(storage.append(earlier, {}), AppendResult::kInserted);
    ASSERT_EQ(storage.append(other, {}), AppendResult::kInserted);
    const auto history = storage.history("app", "key");
    ASSERT_EQ(history.size(), 4);
    EXPECT_EQ(history[0].uuid(), earlier.uuid());
    EXPECT_EQ(history[1].uuid(), lower_server.uuid());
    EXPECT_EQ(history[2].uuid(), lower_uuid.uuid());
    EXPECT_EQ(history[3].uuid(), deletion.uuid());
    EXPECT_THROW(storage.recycle("app", "key", 0), std::invalid_argument);
    EXPECT_EQ(storage.recycle("app", "key", 1), 3);

    const auto retained = storage.history("app", "key");
    ASSERT_EQ(retained.size(), 1);
    EXPECT_EQ(retained[0].uuid(), deletion.uuid());
    EXPECT_TRUE(std::holds_alternative<EntityMessage::Delete>(retained[0].payload()));
    EXPECT_TRUE(storage.find_message(other.uuid()));
  }

  TEST_F(SqliteStorageTest, RollsBackRecyclingWhenAckCleanupFails)
  {
    SqliteStorage storage{_path};
    const auto first = update(1, "first");
    const auto second = update(2, "second");
    const auto newest = update(3, "newest");
    const std::array<std::int32_t, 1> targets{7};
    ASSERT_EQ(storage.append(first, targets), AppendResult::kInserted);
    ASSERT_EQ(storage.append(second, targets), AppendResult::kInserted);
    ASSERT_EQ(storage.append(newest, {}), AppendResult::kInserted);
    ASSERT_TRUE(storage.mark_acked(first.uuid(), 7));
    ASSERT_TRUE(storage.mark_acked(second.uuid(), 7));
    ASSERT_NO_FATAL_FAILURE(execute_sql(R"sql(
      CREATE TRIGGER reject_last_ack_delete BEFORE DELETE ON ack
      WHEN (SELECT count(*) FROM ack) = 1
      BEGIN
        SELECT RAISE(ABORT, 'test failed second ACK delete');
      END;
    )sql"));

    ASSERT_THROW(storage.recycle("app", "key", 1), std::runtime_error);
    const auto history = storage.history("app", "key");
    ASSERT_EQ(history.size(), 3);
    EXPECT_EQ(std::get<EntityMessage::Update>(history[0].payload()).value, "first");
    EXPECT_EQ(std::get<EntityMessage::Update>(history[1].payload()).value, "second");

    ASSERT_NO_FATAL_FAILURE(execute_sql("DROP TRIGGER reject_last_ack_delete"));
    ASSERT_EQ(storage.recycle("app", "key", 1), 2);
    const auto retained = storage.history("app", "key");
    ASSERT_EQ(retained.size(), 1);
    EXPECT_EQ(retained[0].uuid(), newest.uuid());
  }

  TEST_F(SqliteStorageTest, KeepsBaseForIncrementInsideRetentionWindow)
  {
    SqliteStorage storage{_path};
    const auto base = update(1, "base");
    const auto superseded = update(2, "superseded");
    const auto delta = increment(base, 3);
    const auto newest = update(4, "newest");
    ASSERT_EQ(storage.append(base, {}), AppendResult::kInserted);
    ASSERT_EQ(storage.append(superseded, {}), AppendResult::kInserted);
    ASSERT_EQ(storage.append(delta, {}), AppendResult::kInserted);
    ASSERT_EQ(storage.append(newest, {}), AppendResult::kInserted);

    ASSERT_EQ(storage.recycle("app", "key", 2), 1);
    const auto history = storage.history("app", "key");
    ASSERT_EQ(history.size(), 3);
    EXPECT_EQ(history[0].uuid(), base.uuid());
    EXPECT_EQ(history[1].uuid(), delta.uuid());
    EXPECT_EQ(std::get<EntityMessage::IncrementalUpdate>(history[1].payload()).base_message_uuid, base.uuid());
    EXPECT_EQ(history[2].uuid(), newest.uuid());

    ASSERT_EQ(storage.recycle("app", "key", 1), 2);
    const auto retained = storage.history("app", "key");
    ASSERT_EQ(retained.size(), 1);
    EXPECT_EQ(retained[0].uuid(), newest.uuid());
  }

  TEST_F(SqliteStorageTest, AcceptsLateIncrementWithRecycledBase)
  {
    const auto full = update(1, "base");
    const auto newer = update(2, "newer");
    const auto late_delta = increment(full, 3);
    const EntityMessage mismatched{full.uuid(), "app", "key", 10, 1, EntityMessage::Update{"different version"}};
    {
      SqliteStorage storage{_path};
      ASSERT_EQ(storage.append(full, {}), AppendResult::kInserted);
      ASSERT_EQ(storage.append(newer, {}), AppendResult::kInserted);
      ASSERT_EQ(storage.recycle("app", "key", 1), 1);
    }

    SqliteStorage storage{_path};
    EXPECT_EQ(storage.append(full, {}), AppendResult::kAlreadyExists);
    const auto recycled = storage.find_message(full.uuid());
    ASSERT_FALSE(recycled);
    EXPECT_EQ(recycled.error(), MessageUnavailable::kRecycled);
    EXPECT_EQ(storage.append(late_delta, {}), AppendResult::kBaseUnavailable);
    EXPECT_EQ(storage.append(late_delta, {}, &mismatched), AppendResult::kBaseUnavailable);

    ASSERT_EQ(storage.append(late_delta, {}, &full), AppendResult::kInserted);
    EXPECT_EQ(storage.recycle("app", "key", 1), 1);
    const auto history = storage.history("app", "key");
    ASSERT_EQ(history.size(), 2);
    EXPECT_EQ(history[0].uuid(), full.uuid());
    EXPECT_EQ(std::get<EntityMessage::Update>(history[0].payload()).value, "base");
    EXPECT_EQ(history[1].uuid(), late_delta.uuid());
  }

  TEST_F(SqliteStorageTest, RollsBackSuppliedBaseWithIncrement)
  {
    SqliteStorage storage{_path};
    const auto full = update(1, "base");
    const auto delta = increment(full, 2);
    const std::array<std::int32_t, 1> targets{7};
    ASSERT_NO_FATAL_FAILURE(execute_sql(R"sql(
      CREATE TRIGGER reject_target BEFORE INSERT ON ack
      BEGIN
        SELECT RAISE(ABORT, 'test failed ACK insert');
      END;
    )sql"));

    EXPECT_THROW(storage.append(delta, targets, &full), std::runtime_error);
    EXPECT_TRUE(storage.list_keys().empty());
    EXPECT_FALSE(storage.find_message(full.uuid()));
    EXPECT_FALSE(storage.find_message(delta.uuid()));
    ASSERT_NO_FATAL_FAILURE(execute_sql("DROP TRIGGER reject_target"));

    EXPECT_EQ(storage.append(delta, targets, &full), AppendResult::kInserted);
    EXPECT_TRUE(storage.find_message(full.uuid()));
    const auto pending = storage.pending_for_node(7);
    ASSERT_EQ(pending.size(), 1);
    EXPECT_EQ(pending[0].uuid(), delta.uuid());
  }

  TEST_F(SqliteStorageTest, RejectsInvalidBaseWithoutWritingAnything)
  {
    SqliteStorage storage{_path};
    const EntityMessage other{Uuid::generate(), "app", "other", 1, 1, EntityMessage::Update{"value"}};
    const EntityMessage deletion{Uuid::generate(), "app", "key", 1, 1, EntityMessage::Delete{}};
    ASSERT_EQ(storage.append(other, {}), AppendResult::kInserted);
    ASSERT_EQ(storage.append(deletion, {}), AppendResult::kInserted);

    EXPECT_EQ(storage.append(increment(other, 2), {}), AppendResult::kBaseUnavailable);
    EXPECT_EQ(storage.append(increment(deletion, 2), {}), AppendResult::kBaseUnavailable);
    const auto history = storage.history("app", "key");
    ASSERT_EQ(history.size(), 1);
    EXPECT_EQ(history[0].uuid(), deletion.uuid());
  }
}
