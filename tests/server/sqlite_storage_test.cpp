#include <gtest/gtest.h>
#include <sqlite3.h>

#include <algorithm>
#include <array>
#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "storage/sqlite_storage.hpp"

namespace
{
  using cipple::Uuid;
  using cipple::entity::AtomicOperation;
  using cipple::entity::EntityMessage;
  using cipple::storage::AppendResult;
  using cipple::storage::MessageUnavailable;
  using cipple::storage::PendingCursor;
  using cipple::storage::PendingId;
  using cipple::storage::SqliteStorage;

  EntityMessage update(const std::int64_t version, std::string value)
  {
    return {Uuid::generate(), "app", "key", version, 1, EntityMessage::Update{std::move(value)}};
  }

  EntityMessage increment(const EntityMessage& base, const std::int64_t version)
  {
    return {
      Uuid::generate(), "app", "key", version, 1,
      EntityMessage::IncrementalUpdate{base.uuid(), AtomicOperation::kRemoveEntry, "removed"}
    };
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
      const auto path{_path.u8string()};
      sqlite3* raw_db{nullptr};
      const auto open_status{
        sqlite3_open_v2(reinterpret_cast<const char*>(path.c_str()), &raw_db,
                        SQLITE_OPEN_READWRITE, nullptr)
      };
      const std::unique_ptr<sqlite3, decltype(&sqlite3_close)> db{raw_db, sqlite3_close};
      ASSERT_EQ(open_status, SQLITE_OK);
      ASSERT_EQ(sqlite3_exec(db.get(), sql, nullptr, nullptr, nullptr), SQLITE_OK) << sqlite3_errmsg(db.get());
    }

    const std::filesystem::path _path{
      std::filesystem::temp_directory_path() / ("cipple-storage-" + Uuid::generate().to_string() + ".db")
    };
  };

  TEST_F(SqliteStorageTest, RestoresHistoryAndPendingTargetsAfterReopen)
  {
    const auto full{update(1, std::string{"a\0b", 3})};
    const auto delta{increment(full, 2)};
    const EntityMessage other{Uuid::generate(), "app", "other", 3, -2, EntityMessage::Update{""}};
    const std::array<std::int32_t, 3> targets{7, 8, 8};
    {
      SqliteStorage storage{_path};
      ASSERT_EQ(storage.append(full, {}), AppendResult::kInserted);
      ASSERT_EQ(storage.append(delta, targets), AppendResult::kInserted);
      ASSERT_EQ(storage.append(other, {}), AppendResult::kInserted);
      EXPECT_EQ(storage.mark_acked_batch(9, std::array{delta.uuid()}), 0u);
      EXPECT_EQ(storage.mark_acked_batch(7, std::array{delta.uuid()}), 1u);
      EXPECT_EQ(storage.mark_acked_batch(7, std::array{delta.uuid()}), 0u);
    }

    const SqliteStorage storage{_path};
    const auto keys{storage.list_keys()};
    ASSERT_EQ(keys.size(), 2);
    EXPECT_EQ(keys[0].application_name, "app");
    EXPECT_EQ(keys[0].key, "key");
    EXPECT_EQ(keys[1].key, "other");
    const auto messages{storage.history("app", "key")};
    ASSERT_EQ(messages.size(), 2);
    EXPECT_EQ(messages[0].uuid(), full.uuid());
    EXPECT_EQ(std::get<EntityMessage::Update>(messages[0].payload()).value, (std::string{"a\0b", 3}));
    EXPECT_EQ(messages[1].uuid(), delta.uuid());
    const auto& stored_delta{std::get<EntityMessage::IncrementalUpdate>(messages[1].payload())};
    EXPECT_EQ(stored_delta.base_message_uuid, full.uuid());
    EXPECT_EQ(stored_delta.atomic_operation, AtomicOperation::kRemoveEntry);
    EXPECT_EQ(stored_delta.value, "removed");
    const auto empty{storage.find_message(other.uuid())};
    ASSERT_TRUE(empty);
    EXPECT_EQ(empty->last_update_server_id(), -2);
    EXPECT_TRUE(std::get<EntityMessage::Update>(empty->payload()).value.empty());
    EXPECT_TRUE(storage.pending_ids_after(7, std::nullopt, 1).empty());
    const auto pending{storage.pending_ids_after(8, std::nullopt, 2)};
    ASSERT_EQ(pending.size(), 1);
    EXPECT_EQ(pending[0].uuid, delta.uuid());
  }

  TEST_F(SqliteStorageTest, RetryPreservesTheFirstVersionAndAckProgress)
  {
    SqliteStorage storage{_path};
    const auto full{update(1, "first")};
    const std::array<std::int32_t, 1> original_targets{7};
    ASSERT_EQ(storage.append(full, original_targets), AppendResult::kInserted);
    ASSERT_EQ(storage.mark_acked_batch(7, std::array{full.uuid()}), 1u);

    const EntityMessage retry{full.uuid(), "app", "key", 20, 2, EntityMessage::Update{"retry"}};
    const std::array<std::int32_t, 2> retry_targets{7, 8};
    EXPECT_EQ(storage.append(retry, retry_targets), AppendResult::kAlreadyExists);
    const auto stored{storage.find_message(full.uuid())};
    ASSERT_TRUE(stored);
    EXPECT_EQ(stored->last_update(), 1);
    EXPECT_EQ(stored->last_update_server_id(), 1);
    EXPECT_EQ(std::get<EntityMessage::Update>(stored->payload()).value, "first");
    EXPECT_TRUE(storage.pending_ids_after(7, std::nullopt, 1).empty());
    EXPECT_TRUE(storage.pending_ids_after(8, std::nullopt, 1).empty());
  }

  TEST_F(SqliteStorageTest, ReplaysLateOlderVersionsAfterThePersistentCursor)
  {
    const auto newer{update(20, "newer")};
    const auto older{update(10, "late")};
    std::int64_t cursor{0};
    {
      SqliteStorage storage{_path};
      ASSERT_EQ(storage.append(newer, {}), AppendResult::kInserted);
      cursor = storage.journal_sequence(newer.uuid());
      ASSERT_GT(cursor, 0);
      ASSERT_EQ(storage.append(older, {}), AppendResult::kInserted);
      ASSERT_EQ(storage.append(older, {}), AppendResult::kAlreadyExists);
    }
    const SqliteStorage reopened{_path};
    const auto catch_up{reopened.journal_after("app", "key", cursor, 10)};
    ASSERT_EQ(catch_up.size(), 1);
    EXPECT_EQ(catch_up.front().message.uuid(), older.uuid());
    EXPECT_GT(catch_up.front().sequence, cursor);
    EXPECT_EQ(reopened.latest("app", "key")->uuid(), newer.uuid());
    EXPECT_EQ(reopened.version_watermark(), newer.last_update());
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
    const auto full{update(1, "value")};
    const std::array<std::int32_t, 2> targets{21, 22};
    EXPECT_THROW(storage.append(full, targets), std::runtime_error);

    const SqliteStorage reopened{_path};
    EXPECT_TRUE(reopened.list_keys().empty());
    const auto lookup{reopened.find_message(full.uuid())};
    ASSERT_FALSE(lookup);
    EXPECT_EQ(lookup.error(), MessageUnavailable::kMissing);
    EXPECT_TRUE(reopened.pending_ids_after(21, std::nullopt, 1).empty());
    EXPECT_TRUE(reopened.pending_ids_after(22, std::nullopt, 1).empty());
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
    const auto full{update(1, "value")};
    const std::array<std::int32_t, 1> targets{7};

    ASSERT_THROW(storage.append(full, targets), std::runtime_error);
    EXPECT_TRUE(storage.list_keys().empty());
    const auto lookup{storage.find_message(full.uuid())};
    ASSERT_FALSE(lookup);
    EXPECT_EQ(lookup.error(), MessageUnavailable::kMissing);
    EXPECT_TRUE(storage.pending_ids_after(7, std::nullopt, 1).empty());

    ASSERT_NO_FATAL_FAILURE(execute_sql("DROP TRIGGER reject_ack_at_commit"));
    ASSERT_EQ(storage.append(full, targets), AppendResult::kInserted);
    const auto pending{storage.pending_ids_after(7, std::nullopt, 2)};
    ASSERT_EQ(pending.size(), 1);
    EXPECT_EQ(pending[0].uuid, full.uuid());
  }

  TEST_F(SqliteStorageTest, KeepsPendingIncrementAndItsBaseUntilAllTargetsAck)
  {
    SqliteStorage storage{_path};
    const auto full{update(1, "base")};
    const auto delta{increment(full, 2)};
    const auto newest{update(3, "newest")};
    const std::array<std::int32_t, 2> targets{7, 8};
    ASSERT_EQ(storage.append(full, {}), AppendResult::kInserted);
    ASSERT_EQ(storage.append(delta, targets), AppendResult::kInserted);
    ASSERT_EQ(storage.append(newest, {}), AppendResult::kInserted);

    EXPECT_EQ(storage.recycle("app", "key", 1), 0);
    ASSERT_EQ(storage.mark_acked_batch(7, std::array{delta.uuid()}), 1u);
    EXPECT_EQ(storage.recycle("app", "key", 1), 0);
    ASSERT_TRUE(storage.find_message(full.uuid()));
    ASSERT_EQ(storage.mark_acked_batch(8, std::array{delta.uuid()}), 1u);
    EXPECT_EQ(storage.recycle("app", "key", 1), 2);

    const auto history{storage.history("app", "key")};
    ASSERT_EQ(history.size(), 1);
    EXPECT_EQ(history[0].uuid(), newest.uuid());
    const auto recycled_delta{storage.find_message(delta.uuid())};
    ASSERT_FALSE(recycled_delta);
    EXPECT_EQ(recycled_delta.error(), MessageUnavailable::kRecycled);
    EXPECT_EQ(storage.append(delta, targets), AppendResult::kAlreadyExists);
    EXPECT_EQ(storage.mark_acked_batch(8, std::array{delta.uuid()}), 0u);
    EXPECT_TRUE(storage.pending_ids_after(8, std::nullopt, 1).empty());
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
    const auto history{storage.history("app", "key")};
    ASSERT_EQ(history.size(), 4);
    EXPECT_EQ(history[0].uuid(), earlier.uuid());
    EXPECT_EQ(history[1].uuid(), lower_server.uuid());
    EXPECT_EQ(history[2].uuid(), lower_uuid.uuid());
    EXPECT_EQ(history[3].uuid(), deletion.uuid());
    EXPECT_THROW(storage.recycle("app", "key", 0), std::invalid_argument);
    EXPECT_EQ(storage.recycle("app", "key", 1), 3);

    const auto retained{storage.history("app", "key")};
    ASSERT_EQ(retained.size(), 1);
    EXPECT_EQ(retained[0].uuid(), deletion.uuid());
    EXPECT_TRUE(std::holds_alternative<EntityMessage::Delete>(retained[0].payload()));
    EXPECT_TRUE(storage.find_message(other.uuid()));
  }

  TEST_F(SqliteStorageTest, RollsBackRecyclingWhenAckCleanupFails)
  {
    SqliteStorage storage{_path};
    const auto first{update(1, "first")};
    const auto second{update(2, "second")};
    const auto newest{update(3, "newest")};
    const std::array<std::int32_t, 1> targets{7};
    ASSERT_EQ(storage.append(first, targets), AppendResult::kInserted);
    ASSERT_EQ(storage.append(second, targets), AppendResult::kInserted);
    ASSERT_EQ(storage.append(newest, {}), AppendResult::kInserted);
    ASSERT_EQ(storage.mark_acked_batch(7, std::array{first.uuid()}), 1u);
    ASSERT_EQ(storage.mark_acked_batch(7, std::array{second.uuid()}), 1u);
    ASSERT_NO_FATAL_FAILURE(execute_sql(R"sql(
      CREATE TRIGGER reject_last_ack_delete BEFORE DELETE ON ack
      WHEN (SELECT count(*) FROM ack) = 1
      BEGIN
        SELECT RAISE(ABORT, 'test failed second ACK delete');
      END;
    )sql"));

    ASSERT_THROW(storage.recycle("app", "key", 1), std::runtime_error);
    const auto history{storage.history("app", "key")};
    ASSERT_EQ(history.size(), 3);
    EXPECT_EQ(std::get<EntityMessage::Update>(history[0].payload()).value, "first");
    EXPECT_EQ(std::get<EntityMessage::Update>(history[1].payload()).value, "second");

    ASSERT_NO_FATAL_FAILURE(execute_sql("DROP TRIGGER reject_last_ack_delete"));
    ASSERT_EQ(storage.recycle("app", "key", 1), 2);
    const auto retained{storage.history("app", "key")};
    ASSERT_EQ(retained.size(), 1);
    EXPECT_EQ(retained[0].uuid(), newest.uuid());
  }

  TEST_F(SqliteStorageTest, KeepsBaseForIncrementInsideRetentionWindow)
  {
    SqliteStorage storage{_path};
    const auto base{update(1, "base")};
    const auto superseded{update(2, "superseded")};
    const auto delta{increment(base, 3)};
    const auto newest{update(4, "newest")};
    ASSERT_EQ(storage.append(base, {}), AppendResult::kInserted);
    ASSERT_EQ(storage.append(superseded, {}), AppendResult::kInserted);
    ASSERT_EQ(storage.append(delta, {}), AppendResult::kInserted);
    ASSERT_EQ(storage.append(newest, {}), AppendResult::kInserted);

    ASSERT_EQ(storage.recycle("app", "key", 2), 1);
    const auto history{storage.history("app", "key")};
    ASSERT_EQ(history.size(), 3);
    EXPECT_EQ(history[0].uuid(), base.uuid());
    EXPECT_EQ(history[1].uuid(), delta.uuid());
    EXPECT_EQ(std::get<EntityMessage::IncrementalUpdate>(history[1].payload()).base_message_uuid, base.uuid());
    EXPECT_EQ(history[2].uuid(), newest.uuid());

    ASSERT_EQ(storage.recycle("app", "key", 1), 2);
    const auto retained{storage.history("app", "key")};
    ASSERT_EQ(retained.size(), 1);
    EXPECT_EQ(retained[0].uuid(), newest.uuid());
  }

  TEST_F(SqliteStorageTest, AcceptsLateIncrementWithRecycledBase)
  {
    const auto full{update(1, "base")};
    const auto newer{update(2, "newer")};
    const auto late_delta{increment(full, 3)};
    const EntityMessage mismatched{full.uuid(), "app", "key", 10, 1, EntityMessage::Update{"different version"}};
    {
      SqliteStorage storage{_path};
      ASSERT_EQ(storage.append(full, {}), AppendResult::kInserted);
      ASSERT_EQ(storage.append(newer, {}), AppendResult::kInserted);
      ASSERT_EQ(storage.recycle("app", "key", 1), 1);
    }

    SqliteStorage storage{_path};
    EXPECT_EQ(storage.append(full, {}), AppendResult::kAlreadyExists);
    const auto recycled{storage.find_message(full.uuid())};
    ASSERT_FALSE(recycled);
    EXPECT_EQ(recycled.error(), MessageUnavailable::kRecycled);
    EXPECT_EQ(storage.append(late_delta, {}), AppendResult::kBaseUnavailable);
    EXPECT_EQ(storage.append(late_delta, {}, &mismatched), AppendResult::kBaseUnavailable);

    ASSERT_EQ(storage.append(late_delta, {}, &full), AppendResult::kInserted);
    EXPECT_EQ(storage.recycle("app", "key", 1), 1);
    const auto history{storage.history("app", "key")};
    ASSERT_EQ(history.size(), 2);
    EXPECT_EQ(history[0].uuid(), full.uuid());
    EXPECT_EQ(std::get<EntityMessage::Update>(history[0].payload()).value, "base");
    EXPECT_EQ(history[1].uuid(), late_delta.uuid());
  }

  TEST_F(SqliteStorageTest, RollsBackSuppliedBaseWithIncrement)
  {
    SqliteStorage storage{_path};
    const auto full{update(1, "base")};
    const auto delta{increment(full, 2)};
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
    const auto pending{storage.pending_ids_after(7, std::nullopt, 2)};
    ASSERT_EQ(pending.size(), 1);
    EXPECT_EQ(pending[0].uuid, delta.uuid());
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
    const auto history{storage.history("app", "key")};
    ASSERT_EQ(history.size(), 1);
    EXPECT_EQ(history[0].uuid(), deletion.uuid());
  }

  std::string schema_catalog(const std::filesystem::path& path)
  {
    const auto utf8{path.u8string()};
    sqlite3* raw{nullptr};
    const auto opened{
      sqlite3_open_v2(reinterpret_cast<const char*>(utf8.c_str()), &raw, SQLITE_OPEN_READONLY,
                      nullptr)
    };
    const std::unique_ptr<sqlite3, decltype(&sqlite3_close)> db{raw, sqlite3_close};
    EXPECT_EQ(opened, SQLITE_OK);
    sqlite3_stmt* statement{nullptr};
    EXPECT_EQ(sqlite3_prepare_v2(db.get(),
                                 "SELECT type, name, COALESCE(sql, '') FROM sqlite_master ORDER BY 1, 2",
                                 -1, &statement, nullptr), SQLITE_OK);
    std::string catalog;
    while (sqlite3_step(statement) == SQLITE_ROW)
    {
      catalog.append(reinterpret_cast<const char*>(sqlite3_column_text(statement, 0)));
      catalog.push_back('|');
      catalog.append(reinterpret_cast<const char*>(sqlite3_column_text(statement, 1)));
      catalog.push_back('|');
      catalog.append(reinterpret_cast<const char*>(sqlite3_column_text(statement, 2)));
      catalog.push_back('\n');
    }
    sqlite3_finalize(statement);
    return catalog;
  }

  TEST_F(SqliteStorageTest, RejectsDatabaseWithMissingReplicationIndex)
  {
    {
      const SqliteStorage storage{_path};
    }
    ASSERT_NO_FATAL_FAILURE(execute_sql("DROP INDEX ack_by_version;"));
    const auto catalog{schema_catalog(_path)};
    EXPECT_THROW(static_cast<void>(SqliteStorage{_path}), std::runtime_error);
    EXPECT_EQ(schema_catalog(_path), catalog);
  }

  std::int64_t statement_steps(sqlite3* db, const char* sql, const std::int64_t node_id, const std::int64_t limit,
                               const bool bounded, const std::int64_t after_version, int* sorts, int* full_scans)
  {
    sqlite3_stmt* statement{nullptr};
    EXPECT_EQ(sqlite3_prepare_v2(db, sql, -1, &statement, nullptr), SQLITE_OK);
    sqlite3_bind_int64(statement, 1, node_id);
    if (bounded)
    {
      sqlite3_bind_int64(statement, 2, after_version);
      sqlite3_bind_int64(statement, 3, 1);
      sqlite3_bind_blob(statement, 4, "", 0, SQLITE_STATIC);
      // A zero-length blob is not a UUID; tests that need a real cursor use the storage API.
      sqlite3_bind_int64(statement, 5, limit);
    }
    else
    {
      sqlite3_bind_int64(statement, 2, limit);
    }
    while (sqlite3_step(statement) == SQLITE_ROW)
    {
    }
    *sorts = sqlite3_stmt_status(statement, SQLITE_STMTSTATUS_SORT, 0);
    *full_scans = sqlite3_stmt_status(statement, SQLITE_STMTSTATUS_FULLSCAN_STEP, 0);
    const auto steps{sqlite3_stmt_status(statement, SQLITE_STMTSTATUS_VM_STEP, 0)};
    sqlite3_finalize(statement);
    return steps;
  }

  TEST_F(SqliteStorageTest, PagesSignedVersionsWithoutASentinelCursor)
  {
    SqliteStorage storage{_path};
    const EntityMessage lowest{ordered_uuid(1), "app", "key", -5, -9, EntityMessage::Update{"low"}};
    const EntityMessage middle{ordered_uuid(2), "app", "key", -5, 4, EntityMessage::Update{"mid"}};
    const EntityMessage highest{ordered_uuid(3), "app", "key", 8, -3, EntityMessage::Update{"high"}};
    const std::array<std::int32_t, 1> targets{7};
    ASSERT_EQ(storage.append(highest, targets), AppendResult::kInserted);
    ASSERT_EQ(storage.append(lowest, targets), AppendResult::kInserted);
    ASSERT_EQ(storage.append(middle, targets), AppendResult::kInserted);

    const auto first{storage.pending_ids_after(7, std::nullopt, 2)};
    ASSERT_EQ(first.size(), 2);
    EXPECT_EQ(first[0].uuid, lowest.uuid());
    EXPECT_EQ(first[0].last_update, -5);
    EXPECT_EQ(first[0].last_update_server_id, -9);
    EXPECT_EQ(first[1].uuid, middle.uuid());

    const PendingCursor after_lowest{first[0].last_update, first[0].last_update_server_id, first[0].uuid};
    const auto rest{storage.pending_ids_after(7, after_lowest, 10)};
    ASSERT_EQ(rest.size(), 2);
    EXPECT_EQ(rest[0].uuid, middle.uuid());
    EXPECT_EQ(rest[1].uuid, highest.uuid());
    EXPECT_EQ(rest[1].last_update_server_id, -3);

    EXPECT_TRUE(storage.pending_ids_after(7, PendingCursor{
                                            rest.back().last_update, rest.back().last_update_server_id,
                                            rest.back().uuid
                                          }, 10).empty());
  }

  TEST_F(SqliteStorageTest, AckBatchIsAtomicAndVisibleOnlyAfterCommit)
  {
    SqliteStorage storage{_path};
    const SqliteStorage reader{_path};
    const auto first{update(1, "one")};
    const auto second{update(2, "two")};
    const auto third{update(3, "three")};
    const std::array<std::int32_t, 1> targets{7};
    ASSERT_EQ(storage.append(first, targets), AppendResult::kInserted);
    ASSERT_EQ(storage.append(second, targets), AppendResult::kInserted);
    ASSERT_EQ(storage.append(third, targets), AppendResult::kInserted);
    const std::vector<Uuid> all{first.uuid(), second.uuid(), third.uuid()};
    EXPECT_TRUE(std::ranges::equal(reader.pending_ids_after(7, std::nullopt, 4), all, {}, &PendingId::uuid));

    ASSERT_NO_FATAL_FAILURE(execute_sql(R"sql(
      CREATE TRIGGER reject_batch_tail BEFORE UPDATE ON ack
      WHEN NEW.node_id = 7 AND NEW.acknowledged = 1
       AND (SELECT count(*) FROM ack WHERE node_id = 7 AND acknowledged = 1) >= 1
      BEGIN
        SELECT RAISE(ABORT, 'stop batch');
      END;
    )sql"));
    EXPECT_THROW(storage.mark_acked_batch(7, std::array{first.uuid(), second.uuid()}), std::runtime_error);
    EXPECT_TRUE(std::ranges::equal(reader.pending_ids_after(7, std::nullopt, 4), all, {}, &PendingId::uuid));
    ASSERT_NO_FATAL_FAILURE(execute_sql("DROP TRIGGER reject_batch_tail"));

    ASSERT_EQ(storage.mark_acked_batch(7, std::array{third.uuid()}), 1u);
    EXPECT_EQ(storage.mark_acked_batch(7, std::array{first.uuid(), second.uuid(), third.uuid()}), 2u);
    EXPECT_EQ(storage.mark_acked_batch(7, std::array{first.uuid(), second.uuid()}), 0u);
    EXPECT_TRUE(reader.pending_ids_after(7, std::nullopt, 1).empty());
    EXPECT_EQ(storage.mark_acked_batch(7, std::span<const Uuid>{}), 0u);
  }

  TEST_F(SqliteStorageTest, PendingPageDoesNotSortTheWholeBacklog)
  {
    constexpr int kRows{4000};
    {
      SqliteStorage storage{_path};
    }
    const auto utf8{_path.u8string()};
    sqlite3* raw{nullptr};
    ASSERT_EQ(sqlite3_open(reinterpret_cast<const char*>(utf8.c_str()), &raw), SQLITE_OK);
    const std::unique_ptr<sqlite3, decltype(&sqlite3_close)> db{raw, sqlite3_close};
    ASSERT_EQ(sqlite3_exec(db.get(), "BEGIN", nullptr, nullptr, nullptr), SQLITE_OK);
    sqlite3_stmt* message{nullptr};
    sqlite3_stmt* ack{nullptr};
    ASSERT_EQ(sqlite3_prepare_v2(db.get(),
                                 "INSERT INTO message(uuid, application_name, key, last_update, last_update_server_id,"
                                 " operation_type, retained, value) VALUES (?1, ?3, ?4, ?2, 1, 'update', 1, ?5)",
                                 -1, &message, nullptr), SQLITE_OK);
    ASSERT_EQ(sqlite3_prepare_v2(db.get(),
                                 "INSERT INTO ack(message_uuid, node_id, last_update, last_update_server_id)"
                                 " VALUES (?1, 7, ?2, 1)",
                                 -1, &ack, nullptr), SQLITE_OK);
    for (int row{0}; row < kRows; ++row)
    {
      std::array < unsigned char, 16 > id{};
      id[14] = static_cast<unsigned char>((row >> 8) & 0xff);
      id[15] = static_cast<unsigned char>(row & 0xff);
      const auto version{static_cast<sqlite3_int64>(row) - 20};
      sqlite3_bind_blob(message, 1, id.data(), 16, SQLITE_TRANSIENT);
      sqlite3_bind_int64(message, 2, version);
      sqlite3_bind_blob(message, 3, "app", 3, SQLITE_TRANSIENT);
      sqlite3_bind_blob(message, 4, "key", 3, SQLITE_TRANSIENT);
      sqlite3_bind_blob(message, 5, "v", 1, SQLITE_TRANSIENT);
      ASSERT_EQ(sqlite3_step(message), SQLITE_DONE) << sqlite3_errmsg(db.get());
      sqlite3_reset(message);
      sqlite3_bind_blob(ack, 1, id.data(), 16, SQLITE_TRANSIENT);
      sqlite3_bind_int64(ack, 2, version);
      ASSERT_EQ(sqlite3_step(ack), SQLITE_DONE);
      sqlite3_reset(ack);
    }
    sqlite3_finalize(message);
    sqlite3_finalize(ack);
    ASSERT_EQ(sqlite3_exec(db.get(), "COMMIT", nullptr, nullptr, nullptr), SQLITE_OK);

    sqlite3_stmt* plan{nullptr};
    ASSERT_EQ(sqlite3_prepare_v2(db.get(),
                                 "EXPLAIN QUERY PLAN "
                                 "SELECT message_uuid FROM ack WHERE node_id = ?1 AND acknowledged = 0 "
                                 "ORDER BY last_update, last_update_server_id, message_uuid LIMIT ?2",
                                 -1, &plan, nullptr), SQLITE_OK);
    sqlite3_bind_int(plan, 1, 7);
    sqlite3_bind_int(plan, 2, 256);
    std::string planned;
    while (sqlite3_step(plan) == SQLITE_ROW)
    {
      planned.append(reinterpret_cast<const char*>(sqlite3_column_text(plan, 3)));
      planned.push_back('\n');
    }
    sqlite3_finalize(plan);
    EXPECT_NE(planned.find("ack_by_version"), std::string::npos) << planned;
    EXPECT_EQ(planned.find("TEMP B-TREE"), std::string::npos) << planned;

    int sorts{0};
    int full_scans{0};
    const auto steps{
      statement_steps(db.get(),
                      "SELECT message_uuid, last_update, last_update_server_id FROM ack "
                      "WHERE node_id = ?1 AND acknowledged = 0 "
                      "ORDER BY last_update, last_update_server_id, message_uuid LIMIT ?2",
                      7, 256, false, 0, &sorts, &full_scans)
    };
    EXPECT_EQ(sorts, 0);
    EXPECT_EQ(full_scans, 0);
    EXPECT_LT(steps, kRows) << "page scan walked the whole backlog";

    SqliteStorage storage{_path};
    const auto page{storage.pending_ids_after(7, std::nullopt, 256)};
    ASSERT_EQ(page.size(), 256u);
    EXPECT_EQ(page.front().last_update, -20);
    const auto next{
      storage.pending_ids_after(7, PendingCursor{
                                  page.back().last_update, page.back().last_update_server_id,
                                  page.back().uuid
                                }, 1)
    };
    ASSERT_EQ(next.size(), 1u);
    EXPECT_EQ(next.front().last_update, page.back().last_update + 1);
  }
}
