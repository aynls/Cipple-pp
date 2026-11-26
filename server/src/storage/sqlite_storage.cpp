#include "sqlite_storage.hpp"

#include <sqlite3.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>

#include "entity/resolve.hpp"
#include "protocol/frame_codec.hpp"

namespace cipple::storage
{
  namespace
  {
    using entity::EntityMessage;
    using entity::OperationType;

    constexpr char kSchema[]{
      R"sql(
      CREATE TABLE item (
        application_name BLOB NOT NULL,
        key BLOB NOT NULL,
        PRIMARY KEY (application_name, key)
      ) WITHOUT ROWID;

      CREATE TABLE message (
        uuid BLOB PRIMARY KEY CHECK (typeof(uuid) = 'blob' AND length(uuid) = 16),
        application_name BLOB NOT NULL,
        key BLOB NOT NULL,
        last_update INTEGER NOT NULL,
        last_update_server_id INTEGER NOT NULL,
        operation_type TEXT NOT NULL CHECK (operation_type IN ('update', 'incremental_update', 'delete')),
        retained INTEGER NOT NULL DEFAULT 1 CHECK (retained IN (0, 1)),
        value BLOB,
        base_message_uuid BLOB REFERENCES message(uuid),
        atomic_operation TEXT CHECK (atomic_operation IN ('add_entry', 'remove_entry')),
        FOREIGN KEY (application_name, key) REFERENCES item(application_name, key),
        CHECK (
          (retained = 1 AND operation_type <> 'delete' AND typeof(value) = 'blob')
          OR ((retained = 0 OR operation_type = 'delete') AND value IS NULL)
        ),
        CHECK (
          (retained = 1 AND operation_type = 'incremental_update'
            AND base_message_uuid IS NOT NULL AND atomic_operation IS NOT NULL)
          OR ((retained = 0 OR operation_type <> 'incremental_update')
            AND base_message_uuid IS NULL AND atomic_operation IS NULL)
        )
      ) WITHOUT ROWID;

      CREATE TABLE ack (
        message_uuid BLOB NOT NULL REFERENCES message(uuid),
        node_id INTEGER NOT NULL,
        acknowledged INTEGER NOT NULL DEFAULT 0 CHECK (acknowledged IN (0, 1)),
        last_update INTEGER NOT NULL,
        last_update_server_id INTEGER NOT NULL,
        PRIMARY KEY (message_uuid, node_id)
      ) WITHOUT ROWID;

      CREATE INDEX message_history
        ON message(application_name, key, last_update, last_update_server_id, uuid)
        WHERE retained = 1;

      CREATE TABLE delivery_log (
        sequence INTEGER PRIMARY KEY,
        message_uuid BLOB NOT NULL UNIQUE REFERENCES message(uuid),
        application_name BLOB NOT NULL,
        key BLOB NOT NULL
      );
      CREATE INDEX delivery_log_topic ON delivery_log(application_name, key, sequence);
      CREATE INDEX ack_by_version
        ON ack(node_id, last_update, last_update_server_id, message_uuid)
        WHERE acknowledged = 0;
    )sql"
    };

    constexpr std::size_t kSha256Length{32};

    constexpr char kRaftSchema[]{
      R"sql(
      CREATE TABLE request_result (
        uuid BLOB PRIMARY KEY CHECK (typeof(uuid) = 'blob' AND length(uuid) = 16),
        command_hash BLOB NOT NULL CHECK (typeof(command_hash) = 'blob' AND length(command_hash) = 32),
        accepted INTEGER NOT NULL CHECK (accepted IN (0, 1)),
        log_index INTEGER NOT NULL
      ) WITHOUT ROWID;

      CREATE TABLE journal_retention (
        application_name BLOB NOT NULL,
        key BLOB NOT NULL,
        floor INTEGER NOT NULL,
        PRIMARY KEY (application_name, key)
      ) WITHOUT ROWID;

      CREATE TABLE raft_state (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        applied_index INTEGER NOT NULL,
        journal_watermark INTEGER NOT NULL,
        snapshot_log_index INTEGER NOT NULL,
        snapshot_log_term INTEGER NOT NULL,
        snapshot_file_size INTEGER NOT NULL,
        snapshot_sha256 BLOB,
        snapshot_metadata BLOB,
        CHECK (
          (snapshot_log_index = 0 AND snapshot_log_term = 0 AND snapshot_file_size = 0
            AND snapshot_sha256 IS NULL AND snapshot_metadata IS NULL)
          OR (snapshot_log_index > 0 AND snapshot_file_size > 0
            AND typeof(snapshot_sha256) = 'blob' AND length(snapshot_sha256) = 32
            AND typeof(snapshot_metadata) = 'blob' AND length(snapshot_metadata) > 0)
        )
      ) WITHOUT ROWID;
    )sql"
    };

    constexpr char kPendingIds[]{
      R"sql(
      SELECT message_uuid, last_update, last_update_server_id
      FROM ack
      WHERE node_id = ?1 AND acknowledged = 0
      ORDER BY last_update, last_update_server_id, message_uuid
      LIMIT ?2
    )sql"
    };

    constexpr char kPendingIdsAfter[]{
      R"sql(
      SELECT message_uuid, last_update, last_update_server_id
      FROM ack
      WHERE node_id = ?1 AND acknowledged = 0
        AND (last_update, last_update_server_id, message_uuid) > (?2, ?3, ?4)
      ORDER BY last_update, last_update_server_id, message_uuid
      LIMIT ?5
    )sql"
    };

    [[noreturn]] void throw_sqlite(sqlite3* db, const int error, const char* operation)
    {
      const auto* detail{db ? sqlite3_errmsg(db) : sqlite3_errstr(error)};
      throw std::runtime_error{std::string{"SQLite "} + operation + " (" + std::to_string(error) + "): " + detail};
    }

    void check(sqlite3* db, const int result, const char* operation)
    {
      if (result != SQLITE_OK)
      {
        throw_sqlite(db, result, operation);
      }
    }

    void execute_sql(sqlite3* db, const char* sql, const char* operation)
    {
      check(db, sqlite3_exec(db, sql, nullptr, nullptr, nullptr), operation);
    }

    class Statement
    {
    public:
      Statement(sqlite3* db, const char* sql)
      {
        check(db, sqlite3_prepare_v2(db, sql, -1, &_stmt, nullptr), "prepare statement");
      }

      ~Statement() noexcept
      {
        sqlite3_finalize(_stmt);
      }

      Statement(const Statement&) = delete;
      Statement& operator=(const Statement&) = delete;

      void bind(const int index, const std::int64_t value) const
      {
        check(db(), sqlite3_bind_int64(_stmt, index, value), "bind integer");
      }

      void bind(const int index, const Uuid& uuid) const
      {
        const auto& bytes{uuid.bytes()};
        bind_bytes(index, {reinterpret_cast<const char*>(bytes.data()), bytes.size()});
      }

      void bind_bytes(const int index, const std::string_view value) const
      {
        const auto* data{value.empty() ? "" : value.data()};
        check(db(), sqlite3_bind_blob64(_stmt, index, data, value.size(), SQLITE_TRANSIENT), "bind bytes");
      }

      void bind_text(const int index, const std::string_view value) const
      {
        const auto* data{value.empty() ? "" : value.data()};
        check(db(), sqlite3_bind_text64(_stmt, index, data, value.size(), SQLITE_TRANSIENT, SQLITE_UTF8),
              "bind text");
      }

      bool next() const
      {
        const auto result{sqlite3_step(_stmt)};
        if (result == SQLITE_ROW)
        {
          return true;
        }
        if (result != SQLITE_DONE)
        {
          throw_sqlite(db(), result, "step statement");
        }
        return false;
      }

      void execute() const
      {
        if (next())
        {
          throw std::runtime_error{"SQLite write unexpectedly returned a row"};
        }
      }

      void reset() const
      {
        check(db(), sqlite3_reset(_stmt), "reset statement");
      }

      bool is_null(const int column) const noexcept
      {
        return sqlite3_column_type(_stmt, column) == SQLITE_NULL;
      }

      std::int64_t integer(const int column) const noexcept
      {
        return sqlite3_column_int64(_stmt, column);
      }

      std::string_view bytes(const int column) const
      {
        const auto* data{static_cast<const char*>(sqlite3_column_blob(_stmt, column))};
        if (data == nullptr)
        {
          if (sqlite3_errcode(db()) == SQLITE_NOMEM)
          {
            throw_sqlite(db(), SQLITE_NOMEM, "read column");
          }
          return {};
        }
        return {data, static_cast<std::size_t>(sqlite3_column_bytes(_stmt, column))};
      }

      Uuid uuid(const int column) const
      {
        const auto value{bytes(column)};
        if (value.size() != 16)
        {
          throw std::runtime_error{"Invalid UUID in SQLite message row"};
        }
        const std::span<const std::uint8_t, 16> uuid_bytes{
          reinterpret_cast<const std::uint8_t*>(value.data()), value.size()
        };
        return Uuid::from_bytes(uuid_bytes);
      }

    private:
      sqlite3* db() const noexcept
      {
        return sqlite3_db_handle(_stmt);
      }

      sqlite3_stmt* _stmt{nullptr};
    };

    class Transaction
    {
    public:
      explicit Transaction(sqlite3* db) : _db{db}
      {
        execute_sql(_db, "BEGIN IMMEDIATE", "begin transaction");
      }

      ~Transaction() noexcept
      {
        if (!sqlite3_get_autocommit(_db))
        {
          sqlite3_exec(_db, "ROLLBACK", nullptr, nullptr, nullptr);
        }
      }

      Transaction(const Transaction&) = delete;
      Transaction& operator=(const Transaction&) = delete;

      void commit() const
      {
        execute_sql(_db, "COMMIT", "commit transaction");
      }

    private:
      sqlite3* _db;
    };

    void bind_key(const Statement& statement, const std::string_view application_name, const std::string_view key)
    {
      // Compare keys byte for byte, including embedded zero bytes.
      statement.bind_bytes(1, application_name);
      statement.bind_bytes(2, key);
    }

    bool message_exists(sqlite3* db, const Uuid& uuid)
    {
      const Statement query{db, "SELECT 1 FROM message WHERE uuid = ?1"};
      query.bind(1, uuid);
      return query.next();
    }

    void insert_item(sqlite3* db, const EntityMessage& message)
    {
      const Statement insert{
        db, R"sql(
        INSERT INTO item(application_name, key) VALUES (?1, ?2)
        ON CONFLICT(application_name, key) DO NOTHING
      )sql"
      };
      bind_key(insert, message.application_name(), message.key());
      insert.execute();
    }

    void record_delivery(sqlite3* db, const EntityMessage& message)
    {
      const Statement insert{
        db, R"sql(
        INSERT INTO delivery_log(application_name, key, message_uuid) VALUES (?1, ?2, ?3)
        ON CONFLICT(message_uuid) DO NOTHING
      )sql"
      };
      bind_key(insert, message.application_name(), message.key());
      insert.bind(3, message.uuid());
      insert.execute();
    }

    void insert_message(sqlite3* db, const EntityMessage& message)
    {
      const Statement insert{
        db, R"sql(
        INSERT INTO message(application_name, key, uuid, last_update, last_update_server_id,
                            operation_type, value, base_message_uuid, atomic_operation)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
      )sql"
      };
      bind_key(insert, message.application_name(), message.key());
      insert.bind(3, message.uuid());
      insert.bind(4, message.last_update());
      insert.bind(5, message.last_update_server_id());
      insert.bind_text(6, entity::operation_type_wire_name(message.operation_type()));
      if (const auto* update{std::get_if<EntityMessage::Update>(&message.payload())})
      {
        insert.bind_bytes(7, update->value);
      }
      else if (const auto* delta{std::get_if<EntityMessage::IncrementalUpdate>(&message.payload())})
      {
        insert.bind_bytes(7, delta->value);
        insert.bind(8, delta->base_message_uuid);
        insert.bind_text(9, entity::atomic_operation_wire_name(delta->atomic_operation));
      }
      insert.execute();
      record_delivery(db, message);
    }

    bool ensure_base(sqlite3* db, const EntityMessage& message, const Uuid& base_uuid,
                     const EntityMessage* supplied_base)
    {
      const auto* supplied_update{
        supplied_base ? std::get_if<EntityMessage::Update>(&supplied_base->payload()) : nullptr
      };
      if (base_uuid == message.uuid() ||
        (supplied_base && (!supplied_update || supplied_base->uuid() != base_uuid ||
          supplied_base->application_name() != message.application_name() ||
          supplied_base->key() != message.key())))
      {
        return false;
      }

      {
        const Statement query{
          db, R"sql(
          SELECT application_name, key, operation_type, last_update, last_update_server_id, retained, value
          FROM message WHERE uuid = ?1
        )sql"
        };
        query.bind(1, base_uuid);
        if (!query.next())
        {
          if (!supplied_update)
          {
            return false;
          }
          insert_message(db, *supplied_base);
          return true;
        }
        if (query.bytes(0) != message.application_name() || query.bytes(1) != message.key() ||
          query.bytes(2) != entity::operation_type_wire_name(OperationType::kUpdate))
        {
          return false;
        }
        if (supplied_base && (query.integer(3) != supplied_base->last_update() ||
          query.integer(4) != supplied_base->last_update_server_id()))
        {
          return false;
        }
        if (query.integer(5) == 1)
        {
          return !supplied_update || query.bytes(6) == supplied_update->value;
        }
      }

      if (!supplied_update)
      {
        return false;
      }
      const Statement restore{db, "UPDATE message SET retained = 1, value = ?1 WHERE uuid = ?2"};
      restore.bind_bytes(1, supplied_update->value);
      restore.bind(2, base_uuid);
      restore.execute();
      record_delivery(db, *supplied_base);
      return true;
    }

    std::string select_messages(const std::string_view suffix, const bool journal = false)
    {
      std::string sql{
        R"sql(
        SELECT m.uuid, m.application_name, m.key, m.last_update, m.last_update_server_id,
               m.operation_type, m.value, m.base_message_uuid, m.atomic_operation, m.retained
      )sql"
      };
      if (journal)
      {
        sql.append(", d.sequence");
      }
      sql.append(" FROM message AS m ");
      sql.append(suffix);
      return sql;
    }

    EntityMessage read_message(const Statement& row)
    {
      EntityMessage::Payload payload;
      const auto operation{entity::operation_type_parse(row.bytes(5))};
      if (operation == OperationType::kUpdate)
      {
        payload = EntityMessage::Update{std::string{row.bytes(6)}};
      }
      else if (operation == OperationType::kIncrementalUpdate)
      {
        const auto atomic_operation{entity::atomic_operation_parse(row.bytes(8))};
        if (!atomic_operation)
        {
          throw std::runtime_error{"Invalid atomic operation in SQLite message row"};
        }
        payload = EntityMessage::IncrementalUpdate{row.uuid(7), *atomic_operation, std::string{row.bytes(6)}};
      }
      else if (operation == OperationType::kDelete)
      {
        payload = EntityMessage::Delete{};
      }
      else
      {
        throw std::runtime_error{"Invalid operation type in SQLite message row"};
      }
      return {
        row.uuid(0), std::string{row.bytes(1)}, std::string{row.bytes(2)},
        row.integer(3), static_cast<std::int32_t>(row.integer(4)), std::move(payload)
      };
    }

    std::vector<EntityMessage> read_messages(const Statement& query)
    {
      std::vector<EntityMessage> messages;
      while (query.next())
      {
        messages.push_back(read_message(query));
      }
      return messages;
    }

    bool is_empty_database(sqlite3* db)
    {
      return !Statement{db, "SELECT 1 FROM sqlite_master LIMIT 1"}.next();
    }

    void check_schema(sqlite3* db)
    {
      execute_sql(db, R"sql(
        SELECT application_name, key FROM item LIMIT 0;
        SELECT uuid, application_name, key, last_update, last_update_server_id,
               operation_type, retained, value, base_message_uuid, atomic_operation
          FROM message INDEXED BY message_history WHERE retained = 1 LIMIT 0;
        SELECT message_uuid, node_id, acknowledged, last_update, last_update_server_id
          FROM ack INDEXED BY ack_by_version WHERE acknowledged = 0 LIMIT 0;
        SELECT sequence, message_uuid, application_name, key
          FROM delivery_log INDEXED BY delivery_log_topic LIMIT 0;
      )sql", "check storage schema");
    }

    bool table_exists(sqlite3* db, const char* name)
    {
      const Statement query{db, "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1"};
      query.bind_text(1, name);
      return query.next();
    }

    std::int64_t scalar_integer(sqlite3* db, const char* sql)
    {
      const Statement query{db, sql};
      if (!query.next())
      {
        throw std::runtime_error{"SQLite scalar query returned no row"};
      }
      return query.integer(0);
    }

    void check_raft_schema(sqlite3* db)
    {
      execute_sql(db, R"sql(
        SELECT uuid, command_hash, accepted, log_index FROM request_result LIMIT 0;
        SELECT application_name, key, floor FROM journal_retention LIMIT 0;
        SELECT id, applied_index, journal_watermark,
               snapshot_log_index, snapshot_log_term, snapshot_file_size,
               snapshot_sha256, snapshot_metadata
          FROM raft_state LIMIT 0;
      )sql", "check Raft storage schema");
    }

    struct RaftStateRow
    {
      std::int64_t applied_index{0};
      std::int64_t journal_watermark{0};
      SqliteStorage::RaftSnapshotInfo snapshot;
    };

    std::array<std::uint8_t, kSha256Length> copy_sha256(const std::string_view bytes, const char* what)
    {
      if (bytes.size() != kSha256Length)
      {
        throw std::runtime_error{std::string{"Invalid SHA-256 length in "} + what};
      }
      std::array<std::uint8_t, kSha256Length> result{};
      std::memcpy(result.data(), bytes.data(), bytes.size());
      return result;
    }

    RaftStateRow read_raft_state(sqlite3* db)
    {
      const Statement query{
        db, R"sql(
        SELECT applied_index, journal_watermark,
               snapshot_log_index, snapshot_log_term, snapshot_file_size,
               snapshot_sha256, snapshot_metadata
        FROM raft_state WHERE id = 1
      )sql"
      };
      if (!query.next())
      {
        throw std::runtime_error{"Raft storage state row is missing"};
      }
      RaftStateRow state{
        .applied_index = query.integer(0),
        .journal_watermark = query.integer(1),
      };
      if (state.applied_index < 0 || state.journal_watermark < 0)
      {
        throw std::runtime_error{"Raft storage state contains a negative counter"};
      }
      const auto snapshot_index{query.integer(2)};
      const auto snapshot_term{query.integer(3)};
      const auto snapshot_size{query.integer(4)};
      if (snapshot_index < 0 || snapshot_term < 0 || snapshot_size < 0)
      {
        throw std::runtime_error{"Raft snapshot state contains a negative value"};
      }
      state.snapshot.log_index = static_cast<std::uint64_t>(snapshot_index);
      state.snapshot.log_term = static_cast<std::uint64_t>(snapshot_term);
      state.snapshot.file_size = static_cast<std::uint64_t>(snapshot_size);
      if (snapshot_index != 0)
      {
        state.snapshot.sha256 = copy_sha256(query.bytes(5), "raft_state.snapshot_sha256");
        const auto metadata{query.bytes(6)};
        state.snapshot.metadata.assign(metadata.begin(), metadata.end());
        if (state.snapshot.metadata.empty())
        {
          throw std::runtime_error{"Raft snapshot metadata is empty"};
        }
      }
      return state;
    }

    void insert_initial_raft_state(sqlite3* db)
    {
      const Statement insert{
        db, R"sql(
        INSERT INTO raft_state(id, applied_index, journal_watermark,
                               snapshot_log_index, snapshot_log_term, snapshot_file_size,
                               snapshot_sha256, snapshot_metadata)
        VALUES (1, 0, 0, 0, 0, 0, NULL, NULL)
      )sql"
      };
      insert.execute();
    }

    auto version_order(const EntityMessage& message)
    {
      return std::tuple{message.last_update(), message.last_update_server_id(), message.uuid()};
    }

    std::size_t entity_size(const EntityMessage& message) noexcept
    {
      auto size{
        16 + 3 * 8 + 8 + 4 + entity::operation_type_wire_name(message.operation_type()).size()
        + message.application_name().size() + message.key().size()
      };
      if (const auto* full{std::get_if<EntityMessage::Update>(&message.payload())})
      {
        size += 8 + full->value.size();
      }
      if (const auto* delta{std::get_if<EntityMessage::IncrementalUpdate>(&message.payload())})
      {
        size += 16 + 2 * 8 + entity::atomic_operation_wire_name(delta->atomic_operation).size()
          + delta->value.size();
      }
      return size;
    }

    // Same conservative size rule as the pre-Raft writer: the materialized GET
    // value and every contributing entity must fit one application frame. A
    // dispatch frame omits the materialized value, so it is smaller whenever
    // this test passes.
    bool fits_read_response(const EntityMessage& message, const std::optional<EntityMessage>& base)
    {
      constexpr std::size_t get_fields{1 + 16 + 3 * 8 + 4};
      auto size{get_fields + message.application_name().size() + message.key().size() + entity_size(message)};
      if (const auto* full{std::get_if<EntityMessage::Update>(&message.payload())})
      {
        size += full->value.size();
      }
      if (const auto* delta{std::get_if<EntityMessage::IncrementalUpdate>(&message.payload())})
      {
        const auto* full{base ? std::get_if<EntityMessage::Update>(&base->payload()) : nullptr};
        if (!full || base->uuid() != delta->base_message_uuid || base->application_name() != message.application_name()
          || base->key() != message.key() || version_order(*base) >= version_order(message))
        {
          return false;
        }
        size += entity_size(*base) + entity::apply_increment(full->value, *delta).size();
      }
      return size <= tcp::kDefaultMaxFrameLength;
    }

    std::optional<EntityMessage> load_retained_message(sqlite3* db, const Uuid& uuid)
    {
      const auto sql{select_messages("WHERE m.uuid = ?1 AND m.retained = 1")};
      const Statement query{db, sql.c_str()};
      query.bind(1, uuid);
      if (!query.next())
      {
        return std::nullopt;
      }
      return read_message(query);
    }

    bool mutation_is_acceptable(sqlite3* db, const EntityMessage& message)
    {
      std::optional<EntityMessage> base;
      if (const auto* delta{std::get_if<EntityMessage::IncrementalUpdate>(&message.payload())})
      {
        base = load_retained_message(db, delta->base_message_uuid);
      }
      return fits_read_response(message, base);
    }

    void insert_raft_message(sqlite3* db, const EntityMessage& message, const std::int64_t sequence)
    {
      insert_item(db, message);
      const Statement insert{
        db, R"sql(
        INSERT INTO message(application_name, key, uuid, last_update, last_update_server_id,
                            operation_type, value, base_message_uuid, atomic_operation)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
      )sql"
      };
      bind_key(insert, message.application_name(), message.key());
      insert.bind(3, message.uuid());
      insert.bind(4, message.last_update());
      insert.bind(5, message.last_update_server_id());
      insert.bind_text(6, entity::operation_type_wire_name(message.operation_type()));
      if (const auto* update{std::get_if<EntityMessage::Update>(&message.payload())})
      {
        insert.bind_bytes(7, update->value);
      }
      else if (const auto* delta{std::get_if<EntityMessage::IncrementalUpdate>(&message.payload())})
      {
        insert.bind_bytes(7, delta->value);
        insert.bind(8, delta->base_message_uuid);
        insert.bind_text(9, entity::atomic_operation_wire_name(delta->atomic_operation));
      }
      insert.execute();

      const Statement delivery{
        db, R"sql(
        INSERT INTO delivery_log(sequence, application_name, key, message_uuid)
        VALUES (?1, ?2, ?3, ?4)
      )sql"
      };
      delivery.bind(1, sequence);
      delivery.bind_bytes(2, message.application_name());
      delivery.bind_bytes(3, message.key());
      delivery.bind(4, message.uuid());
      delivery.execute();
    }

    void trim_raft_topic(sqlite3* db, const std::string_view application_name, const std::string_view key)
    {
      struct Row
      {
        std::int64_t sequence;
        Uuid uuid;
        std::optional<Uuid> base_uuid;
      };

      std::vector<Row> rows;
      {
        const Statement query{
          db, R"sql(
          SELECT d.sequence, m.uuid, m.base_message_uuid
          FROM delivery_log AS d JOIN message AS m ON m.uuid = d.message_uuid
          WHERE d.application_name = ?1 AND d.key = ?2
          ORDER BY d.sequence DESC
        )sql"
        };
        bind_key(query, application_name, key);
        while (query.next())
        {
          std::optional<Uuid> base;
          if (!query.is_null(2))
          {
            base = query.uuid(2);
          }
          rows.push_back({query.integer(0), query.uuid(1), base});
        }
      }
      if (rows.size() <= SqliteStorage::kRaftRetainedEvents)
      {
        return;
      }

      std::unordered_set<Uuid> protected_bases;
      for (std::size_t i{0}; i < SqliteStorage::kRaftRetainedEvents; ++i)
      {
        if (rows[i].base_uuid)
        {
          protected_bases.insert(*rows[i].base_uuid);
        }
      }

      const Statement delete_delivery{db, "DELETE FROM delivery_log WHERE sequence = ?1"};
      const Statement reclaim{
        db, R"sql(
        UPDATE message SET retained = 0, value = NULL, base_message_uuid = NULL, atomic_operation = NULL
        WHERE uuid = ?1 AND retained = 1
          AND NOT EXISTS (SELECT 1 FROM delivery_log WHERE message_uuid = ?1)
      )sql"
      };
      const Statement clear_ack{db, "DELETE FROM ack WHERE message_uuid = ?1"};
      std::unordered_set<Uuid> candidates;
      for (std::size_t i{SqliteStorage::kRaftRetainedEvents}; i < rows.size(); ++i)
      {
        const auto& row{rows[i]};
        delete_delivery.bind(1, row.sequence);
        delete_delivery.execute();
        delete_delivery.reset();
        candidates.insert(row.uuid);
        if (row.base_uuid)
        {
          // A full version may have left the journal while this delta kept it alive.
          candidates.insert(*row.base_uuid);
        }
      }
      for (const auto& uuid : candidates)
      {
        if (protected_bases.contains(uuid))
        {
          continue;
        }
        reclaim.bind(1, uuid);
        reclaim.execute();
        if (sqlite3_changes(db) != 0)
        {
          clear_ack.bind(1, uuid);
          clear_ack.execute();
          clear_ack.reset();
        }
        reclaim.reset();
      }

      const Statement floor{
        db, R"sql(
        INSERT INTO journal_retention(application_name, key, floor) VALUES (?1, ?2, ?3)
        ON CONFLICT(application_name, key) DO UPDATE SET
          floor = MAX(journal_retention.floor, excluded.floor)
      )sql"
      };
      bind_key(floor, application_name, key);
      floor.bind(3, rows[SqliteStorage::kRaftRetainedEvents].sequence);
      floor.execute();
    }

    struct StoredRequestResult
    {
      std::array<std::uint8_t, kSha256Length> command_hash{};
      bool accepted{false};
    };

    std::optional<StoredRequestResult> find_request_result(sqlite3* db, const Uuid& uuid)
    {
      const Statement query{db, "SELECT command_hash, accepted FROM request_result WHERE uuid = ?1"};
      query.bind(1, uuid);
      if (!query.next())
      {
        return std::nullopt;
      }
      return StoredRequestResult{copy_sha256(query.bytes(0), "request_result.command_hash"), query.integer(1) != 0};
    }

    void insert_request_result(sqlite3* db, const Uuid& uuid, const std::span<const std::uint8_t> command_hash,
                               const bool accepted, const std::int64_t log_index)
    {
      if (command_hash.size() != kSha256Length)
      {
        throw std::invalid_argument{"Raft command hash must be SHA-256"};
      }
      const Statement insert{
        db, "INSERT INTO request_result(uuid, command_hash, accepted, log_index) VALUES (?1, ?2, ?3, ?4)"
      };
      insert.bind(1, uuid);
      insert.bind_bytes(2, {reinterpret_cast<const char*>(command_hash.data()), command_hash.size()});
      insert.bind(3, accepted ? 1 : 0);
      insert.bind(4, log_index);
      insert.execute();
    }

    void complete_raft_apply(sqlite3* db, const RaftStateRow& state, const std::int64_t log_index,
                             const std::optional<std::int64_t> generated_event = std::nullopt)
    {
      if (log_index <= state.applied_index)
      {
        throw std::runtime_error{"Raft log application order regressed"};
      }
      const char* sql{
        generated_event
          ? "UPDATE raft_state SET applied_index = ?1, journal_watermark = MAX(journal_watermark, ?2) WHERE id = 1"
          : "UPDATE raft_state SET applied_index = ?1 WHERE id = 1"
      };
      const Statement update{db, sql};
      update.bind(1, log_index);
      if (generated_event)
      {
        update.bind(2, *generated_event);
      }
      update.execute();
      if (sqlite3_changes(db) != 1)
      {
        throw std::runtime_error{"Raft applied index update did not affect exactly one row"};
      }
    }

    void validate_snapshot_info(const SqliteStorage::RaftSnapshotInfo& snapshot)
    {
      constexpr auto max{static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())};
      if (snapshot.log_index == 0 || snapshot.log_index > max || snapshot.log_term > max ||
        snapshot.file_size == 0 || snapshot.file_size > max || snapshot.metadata.empty())
      {
        throw std::invalid_argument{"invalid Raft snapshot metadata"};
      }
    }
  }

  SqliteStorage::SqliteStorage(const std::filesystem::path& path)
    : SqliteStorage{path, OpenOptions{}}
  {
  }

  SqliteStorage::SqliteStorage(const std::filesystem::path& path, const OpenOptions& options)
  {
    const auto utf8_path{path.u8string()};
    sqlite3* db{nullptr};
    const auto result{
      sqlite3_open_v2(reinterpret_cast<const char*>(utf8_path.c_str()), &db,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr)
    };
    _db.reset(db);
    check(db, result, "open database");
    check(db, sqlite3_busy_timeout(db, 5000), "set busy timeout");
    const bool initialize{is_empty_database(db)};
    if (!initialize)
    {
      check_schema(db);
    }
    {
      const Statement journal_mode{db, "PRAGMA journal_mode = WAL"};
      if (!journal_mode.next() || journal_mode.bytes(0) != "wal")
      {
        throw std::runtime_error{"SQLite storage requires WAL mode"};
      }
    }
    execute_sql(db, "PRAGMA synchronous = FULL", "set synchronous mode");
    execute_sql(db, "PRAGMA foreign_keys = ON", "enable foreign keys");
    {
      const Statement foreign_keys{db, "PRAGMA foreign_keys"};
      if (!foreign_keys.next() || foreign_keys.integer(0) != 1)
      {
        throw std::runtime_error{"SQLite storage requires foreign key support"};
      }
    }
    if (initialize)
    {
      const Transaction transaction{db};
      // A concurrent opener may already have initialized this file.
      if (is_empty_database(db))
      {
        execute_sql(db, kSchema, "initialize storage schema");
      }
      else
      {
        check_schema(db);
      }
      transaction.commit();
    }

    if (options.raft)
    {
      _raft_mode = true;
      if (!table_exists(db, "raft_state"))
      {
        const Transaction transaction{db};
        if (!table_exists(db, "raft_state"))
        {
          if (scalar_integer(db, "SELECT EXISTS(SELECT 1 FROM message)") != 0)
          {
            throw std::runtime_error{"cannot initialize Raft state over existing configuration events"};
          }
          execute_sql(db, kRaftSchema, "initialize Raft storage schema");
          insert_initial_raft_state(db);
        }
        check_raft_schema(db);
        transaction.commit();
      }
      else
      {
        check_raft_schema(db);
      }
    }
  }

  SqliteStorage::~SqliteStorage() noexcept = default;

  void SqliteStorage::CloseDatabase::operator()(sqlite3* db) const noexcept
  {
    sqlite3_close(db);
  }

  AppendResult SqliteStorage::append(const EntityMessage& message,
                                     const std::span<const std::int32_t> target_node_ids,
                                     const EntityMessage* supplied_base)
  {
    return append_impl(message, target_node_ids, supplied_base, false).result;
  }

  AppendOutcome SqliteStorage::append_with_outcome(const EntityMessage& message,
                                                   const std::span<const std::int32_t> target_node_ids,
                                                   const EntityMessage* supplied_base)
  {
    return append_impl(message, target_node_ids, supplied_base, true);
  }

  AppendOutcome SqliteStorage::append_impl(const EntityMessage& message,
                                           const std::span<const std::int32_t> target_node_ids,
                                           const EntityMessage* supplied_base, const bool check_current)
  {
    const std::lock_guard lock{_mutex};
    auto* db{_db.get()};
    const Transaction transaction{db};
    if (message_exists(db, message.uuid()))
    {
      return {AppendResult::kAlreadyExists};
    }

    insert_item(db, message);
    const auto* delta{std::get_if<EntityMessage::IncrementalUpdate>(&message.payload())};
    if (delta && !ensure_base(db, message, delta->base_message_uuid, supplied_base))
    {
      return {AppendResult::kBaseUnavailable};
    }
    insert_message(db, message);
    if (!target_node_ids.empty())
    {
      const Statement insert_ack{
        db, R"sql(
        INSERT INTO ack(message_uuid, node_id, last_update, last_update_server_id)
        VALUES (?1, ?2, ?3, ?4)
        ON CONFLICT(message_uuid, node_id) DO NOTHING
      )sql"
      };
      insert_ack.bind(1, message.uuid());
      insert_ack.bind(3, message.last_update());
      insert_ack.bind(4, message.last_update_server_id());
      for (const auto node_id : target_node_ids)
      {
        insert_ack.bind(2, node_id);
        insert_ack.execute();
        insert_ack.reset();
      }
    }
    AppendOutcome outcome{AppendResult::kInserted};
    if (check_current)
    {
      const Statement latest{
        db, R"sql(
        SELECT uuid FROM message
        WHERE application_name = ?1 AND key = ?2 AND retained = 1
        ORDER BY last_update DESC, last_update_server_id DESC, uuid DESC
        LIMIT 1
      )sql"
      };
      bind_key(latest, message.application_name(), message.key());
      outcome.is_current = latest.next() && latest.uuid(0) == message.uuid();
    }
    transaction.commit();
    return outcome;
  }

  std::vector<ItemKey> SqliteStorage::list_keys() const
  {
    const std::lock_guard lock{_mutex};
    const Statement query{_db.get(), "SELECT application_name, key FROM item ORDER BY application_name, key"};
    std::vector<ItemKey> keys;
    while (query.next())
    {
      keys.push_back({std::string{query.bytes(0)}, std::string{query.bytes(1)}});
    }
    return keys;
  }

  std::vector<EntityMessage> SqliteStorage::history(const std::string_view application_name,
                                                    const std::string_view key) const
  {
    const std::lock_guard lock{_mutex};
    const auto sql{
      select_messages(R"sql(
      WHERE m.application_name = ?1 AND m.key = ?2 AND m.retained = 1
      ORDER BY m.last_update, m.last_update_server_id, m.uuid
    )sql")
    };
    const Statement query{_db.get(), sql.c_str()};
    bind_key(query, application_name, key);
    return read_messages(query);
  }

  std::optional<EntityMessage> SqliteStorage::latest(const std::string_view application_name,
                                                     const std::string_view key) const
  {
    const std::lock_guard lock{_mutex};
    const auto sql{
      select_messages(R"sql(
      WHERE m.application_name = ?1 AND m.key = ?2 AND m.retained = 1
      ORDER BY m.last_update DESC, m.last_update_server_id DESC, m.uuid DESC
      LIMIT 1
    )sql")
    };
    const Statement query{_db.get(), sql.c_str()};
    bind_key(query, application_name, key);
    if (!query.next())
    {
      return std::nullopt;
    }
    return read_message(query);
  }

  std::expected<EntityMessage, MessageUnavailable> SqliteStorage::find_message(const Uuid& uuid) const
  {
    const std::lock_guard lock{_mutex};
    const auto sql{select_messages("WHERE m.uuid = ?1")};
    const Statement query{_db.get(), sql.c_str()};
    query.bind(1, uuid);
    if (!query.next())
    {
      return std::unexpected{MessageUnavailable::kMissing};
    }
    if (query.integer(9) == 0)
    {
      return std::unexpected{MessageUnavailable::kRecycled};
    }
    return read_message(query);
  }

  std::vector<PendingId> SqliteStorage::pending_ids_after(const std::int32_t node_id,
                                                          const std::optional<PendingCursor>& after,
                                                          const std::int64_t limit) const
  {
    if (limit <= 0)
    {
      return {};
    }
    const std::lock_guard lock{_mutex};
    const char* const sql{after ? static_cast<const char*>(kPendingIdsAfter) : static_cast<const char*>(kPendingIds)};
    const Statement query{_db.get(), sql};
    query.bind(1, node_id);
    if (after)
    {
      query.bind(2, after->last_update);
      query.bind(3, after->last_update_server_id);
      query.bind(4, after->uuid);
      query.bind(5, limit);
    }
    else
    {
      query.bind(2, limit);
    }
    std::vector<PendingId> result;
    while (query.next())
    {
      result.push_back(PendingId{query.uuid(0), query.integer(1), static_cast<std::int32_t>(query.integer(2))});
    }
    return result;
  }

  std::size_t SqliteStorage::mark_acked_batch(const std::int32_t node_id, const std::span<const Uuid> message_uuids)
  {
    if (message_uuids.empty())
    {
      return 0;
    }
    const std::lock_guard lock{_mutex};
    auto* db{_db.get()};
    const Transaction transaction{db};
    const Statement update{
      db, R"sql(
      UPDATE ack SET acknowledged = 1
      WHERE message_uuid = ?1 AND node_id = ?2 AND acknowledged = 0
    )sql"
    };
    update.bind(2, node_id);
    std::size_t changed{0};
    for (const auto& message_uuid : message_uuids)
    {
      update.bind(1, message_uuid);
      update.execute();
      if (sqlite3_changes(db) != 0)
      {
        ++changed;
      }
      update.reset();
    }
    transaction.commit();
    return changed;
  }

  std::vector<JournalEntry> SqliteStorage::journal_after(const std::string_view application_name,
                                                         const std::string_view key, const std::int64_t after,
                                                         const std::int64_t limit) const
  {
    const std::lock_guard lock{_mutex};
    const auto sql{
      select_messages(R"sql(
      JOIN delivery_log AS d ON d.message_uuid = m.uuid
      WHERE d.application_name = ?1 AND d.key = ?2 AND d.sequence > ?3 AND m.retained = 1
      ORDER BY d.sequence
      LIMIT ?4
    )sql", true)
    };
    const Statement query{_db.get(), sql.c_str()};
    bind_key(query, application_name, key);
    query.bind(3, after);
    query.bind(4, limit);
    std::vector<JournalEntry> result;
    while (query.next())
    {
      result.push_back({query.integer(10), read_message(query)});
    }
    return result;
  }

  std::int64_t SqliteStorage::journal_sequence(const Uuid& uuid) const
  {
    const std::lock_guard lock{_mutex};
    const Statement query{_db.get(), "SELECT sequence FROM delivery_log WHERE message_uuid = ?1"};
    query.bind(1, uuid);
    return query.next() ? query.integer(0) : 0;
  }

  std::int64_t SqliteStorage::version_watermark() const
  {
    const std::lock_guard lock{_mutex};
    const Statement query{_db.get(), "SELECT COALESCE(MAX(last_update), 0) FROM message"};
    return query.next() ? query.integer(0) : 0;
  }

  std::size_t SqliteStorage::recycle(const std::string_view application_name, const std::string_view key,
                                     const std::size_t keep_latest)
  {
    if (keep_latest == 0)
    {
      throw std::invalid_argument{"keep_latest must be positive"};
    }
    const std::lock_guard lock{_mutex};
    auto* db{_db.get()};
    const Transaction transaction{db};
    std::unordered_set<Uuid> protected_bases;
    std::vector<Uuid> candidates;
    {
      const Statement query{
        db, R"sql(
        SELECT m.uuid, m.base_message_uuid,
               EXISTS (SELECT 1 FROM ack AS a WHERE a.message_uuid = m.uuid AND a.acknowledged = 0)
        FROM message AS m
        WHERE m.application_name = ?1 AND m.key = ?2 AND m.retained = 1
        ORDER BY m.last_update DESC, m.last_update_server_id DESC, m.uuid DESC
      )sql"
      };
      bind_key(query, application_name, key);
      std::size_t rank{0};
      while (query.next())
      {
        const bool keep{rank < keep_latest || query.integer(2) != 0};
        ++rank;
        if (!keep)
        {
          candidates.push_back(query.uuid(0));
        }
        else if (!query.is_null(1))
        {
          // Every retained increment references a single full update.
          protected_bases.insert(query.uuid(1));
        }
      }
    }

    const Statement reclaim{
      db, R"sql(
      UPDATE message SET retained = 0, value = NULL, base_message_uuid = NULL, atomic_operation = NULL
      WHERE uuid = ?1
    )sql"
    };
    const Statement clear_ack{db, "DELETE FROM ack WHERE message_uuid = ?1 AND acknowledged = 1"};
    std::size_t reclaimed{0};
    for (const auto& uuid : candidates)
    {
      if (protected_bases.contains(uuid))
      {
        continue;
      }
      reclaim.bind(1, uuid);
      reclaim.execute();
      reclaim.reset();
      clear_ack.bind(1, uuid);
      clear_ack.execute();
      clear_ack.reset();
      ++reclaimed;
    }
    transaction.commit();
    return reclaimed;
  }

  SqliteStorage::RaftMutationResult SqliteStorage::apply_raft_mutation(
    const EntityMessage& message, const std::span<const std::uint8_t> command_hash, const std::int64_t log_index)
  {
    if (!_raft_mode)
    {
      throw std::logic_error{"Raft mutation applied to non-Raft storage"};
    }
    if (command_hash.size() != kSha256Length)
    {
      throw std::invalid_argument{"Raft command hash must be SHA-256"};
    }
    const std::lock_guard lock{_mutex};
    auto* db{_db.get()};
    const Transaction transaction{db};
    const auto state{read_raft_state(db)};
    if (log_index <= state.applied_index)
    {
      throw std::runtime_error{"Raft log application order regressed"};
    }
    if (message.last_update() != log_index || message.last_update_server_id() != 0)
    {
      throw std::logic_error{"Raft entity revision does not match its log index"};
    }

    bool success{false};
    bool data_changed{false};
    const auto previous{find_request_result(db, message.uuid())};
    if (previous)
    {
      success = std::equal(command_hash.begin(), command_hash.end(), previous->command_hash.begin())
        && previous->accepted;
    }
    else
    {
      success = mutation_is_acceptable(db, message);
      if (success)
      {
        insert_raft_message(db, message, log_index);
        trim_raft_topic(db, message.application_name(), message.key());
        data_changed = true;
      }
      insert_request_result(db, message.uuid(), command_hash, success, log_index);
    }

    complete_raft_apply(db, state, log_index, data_changed ? std::optional{log_index} : std::nullopt);
    transaction.commit();
    return {success, data_changed};
  }

  SqliteStorage::RaftGetResult SqliteStorage::apply_raft_get(const std::string_view application_name,
                                                             const std::string_view key,
                                                             const std::int64_t log_index)
  {
    if (!_raft_mode)
    {
      throw std::logic_error{"Raft read applied to non-Raft storage"};
    }
    const std::lock_guard lock{_mutex};
    auto* db{_db.get()};
    const Transaction transaction{db};
    const auto state{read_raft_state(db)};
    if (log_index <= state.applied_index)
    {
      throw std::runtime_error{"Raft log application order regressed"};
    }

    RaftGetResult result;
    const auto sql{
      select_messages(R"sql(
      WHERE m.application_name = ?1 AND m.key = ?2 AND m.retained = 1
      ORDER BY m.last_update DESC, m.last_update_server_id DESC, m.uuid DESC
      LIMIT 1
    )sql")
    };
    const Statement query{db, sql.c_str()};
    bind_key(query, application_name, key);
    if (query.next())
    {
      auto latest{read_message(query)};
      if (const auto* update{std::get_if<EntityMessage::Update>(&latest.payload())})
      {
        result.value = update->value;
      }
      else if (const auto* increment{std::get_if<EntityMessage::IncrementalUpdate>(&latest.payload())})
      {
        auto base{load_retained_message(db, increment->base_message_uuid)};
        const auto* full{base ? std::get_if<EntityMessage::Update>(&base->payload()) : nullptr};
        if (!full || base->application_name() != application_name || base->key() != key)
        {
          throw std::runtime_error{"stored incremental update has no full base"};
        }
        result.value = entity::apply_increment(full->value, *increment);
        result.versions.push_back(std::move(*base));
      }
      result.versions.push_back(std::move(latest));
    }

    complete_raft_apply(db, state, log_index);
    transaction.commit();
    return result;
  }

  void SqliteStorage::apply_raft_progress(const std::int64_t log_index)
  {
    if (!_raft_mode)
    {
      throw std::logic_error{"Raft progress applied to non-Raft storage"};
    }
    const std::lock_guard lock{_mutex};
    auto* db{_db.get()};
    const Transaction transaction{db};
    const auto state{read_raft_state(db)};
    complete_raft_apply(db, state, log_index);
    transaction.commit();
  }

  std::int64_t SqliteStorage::raft_applied_index() const
  {
    if (!_raft_mode)
    {
      throw std::logic_error{"Raft applied index requested from non-Raft storage"};
    }
    const std::lock_guard lock{_mutex};
    return read_raft_state(_db.get()).applied_index;
  }

  std::optional<SqliteStorage::RaftSnapshotInfo> SqliteStorage::raft_snapshot_info() const
  {
    if (!_raft_mode)
    {
      throw std::logic_error{"Raft snapshot requested from non-Raft storage"};
    }
    const std::lock_guard lock{_mutex};
    auto snapshot{read_raft_state(_db.get()).snapshot};
    if (snapshot.log_index == 0)
    {
      return std::nullopt;
    }
    return snapshot;
  }

  void SqliteStorage::record_raft_snapshot(const RaftSnapshotInfo& snapshot)
  {
    if (!_raft_mode)
    {
      throw std::logic_error{"Raft snapshot recorded in non-Raft storage"};
    }
    validate_snapshot_info(snapshot);
    const std::lock_guard lock{_mutex};
    auto* db{_db.get()};
    const Transaction transaction{db};
    const Statement update{
      db, R"sql(
      UPDATE raft_state SET snapshot_log_index = ?1, snapshot_log_term = ?2,
                            snapshot_file_size = ?3, snapshot_sha256 = ?4, snapshot_metadata = ?5
      WHERE id = 1
    )sql"
    };
    update.bind(1, static_cast<std::int64_t>(snapshot.log_index));
    update.bind(2, static_cast<std::int64_t>(snapshot.log_term));
    update.bind(3, static_cast<std::int64_t>(snapshot.file_size));
    const std::string_view digest{reinterpret_cast<const char*>(snapshot.sha256.data()), snapshot.sha256.size()};
    update.bind_bytes(4, digest);
    const std::string_view metadata{reinterpret_cast<const char*>(snapshot.metadata.data()), snapshot.metadata.size()};
    update.bind_bytes(5, metadata);
    update.execute();
    if (sqlite3_changes(db) != 1)
    {
      throw std::runtime_error{"Raft snapshot metadata update did not affect exactly one row"};
    }
    transaction.commit();
  }

  void SqliteStorage::backup_database(const std::filesystem::path& destination) const
  {
    const std::lock_guard lock{_mutex};
    const auto utf8_path{destination.u8string()};
    sqlite3* raw_destination{nullptr};
    const auto open_result{
      sqlite3_open_v2(reinterpret_cast<const char*>(utf8_path.c_str()), &raw_destination,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr)
    };
    const std::unique_ptr<sqlite3, decltype(&sqlite3_close)> destination_db{raw_destination, sqlite3_close};
    check(raw_destination, open_result, "open backup destination");
    execute_sql(raw_destination, "PRAGMA synchronous = FULL", "set backup synchronous mode");

    auto* raw_backup{
      sqlite3_backup_init(raw_destination, "main", _db.get(), "main")
    };
    if (raw_backup == nullptr)
    {
      throw_sqlite(raw_destination, sqlite3_errcode(raw_destination), "initialize backup");
    }
    std::unique_ptr<sqlite3_backup, decltype(&sqlite3_backup_finish)> backup{raw_backup, sqlite3_backup_finish};
    int step{SQLITE_OK};
    std::size_t busy_retries{0};
    while (step == SQLITE_OK || step == SQLITE_BUSY || step == SQLITE_LOCKED)
    {
      step = sqlite3_backup_step(backup.get(), 512);
      if (step == SQLITE_BUSY || step == SQLITE_LOCKED)
      {
        if (++busy_retries > 10'000)
        {
          throw_sqlite(raw_destination, step, "wait for backup database access");
        }
        sqlite3_sleep(1);
      }
    }
    const auto finish_result{sqlite3_backup_finish(backup.release())};
    if (step != SQLITE_DONE)
    {
      throw_sqlite(raw_destination, step, "copy database backup");
    }
    if (finish_result != SQLITE_OK)
    {
      throw_sqlite(raw_destination, finish_result, "finish database backup");
    }
  }

  void SqliteStorage::restore_raft_snapshot(const std::filesystem::path& image, const RaftSnapshotInfo& snapshot)
  {
    if (!_raft_mode)
    {
      throw std::logic_error{"Raft snapshot restored into non-Raft storage"};
    }
    validate_snapshot_info(snapshot);
    {
      const std::lock_guard lock{_mutex};
      const auto utf8_path{image.u8string()};
      sqlite3* raw_source{nullptr};
      const auto open_result{
        sqlite3_open_v2(reinterpret_cast<const char*>(utf8_path.c_str()), &raw_source,
                        SQLITE_OPEN_READONLY, nullptr)
      };
      const std::unique_ptr<sqlite3, decltype(&sqlite3_close)> source_db{raw_source, sqlite3_close};
      check(raw_source, open_result, "open Raft snapshot image");
      check_raft_schema(raw_source);
      if (read_raft_state(raw_source).applied_index != static_cast<std::int64_t>(snapshot.log_index))
      {
        throw std::runtime_error{"Raft snapshot image applied index does not match its metadata"};
      }

      auto* raw_backup{
        sqlite3_backup_init(_db.get(), "main", raw_source, "main")
      };
      if (raw_backup == nullptr)
      {
        throw_sqlite(_db.get(), sqlite3_errcode(_db.get()), "initialize Raft snapshot restore");
      }
      std::unique_ptr<sqlite3_backup, decltype(&sqlite3_backup_finish)> backup{raw_backup, sqlite3_backup_finish};
      int step{SQLITE_OK};
      std::size_t busy_retries{0};
      while (step == SQLITE_OK || step == SQLITE_BUSY || step == SQLITE_LOCKED)
      {
        step = sqlite3_backup_step(backup.get(), 512);
        if (step == SQLITE_BUSY || step == SQLITE_LOCKED)
        {
          if (++busy_retries > 10'000)
          {
            throw_sqlite(_db.get(), step, "wait for Raft snapshot restore access");
          }
          sqlite3_sleep(1);
        }
      }
      const auto finish_result{sqlite3_backup_finish(backup.release())};
      if (step != SQLITE_DONE)
      {
        throw_sqlite(_db.get(), step, "restore Raft snapshot image");
      }
      if (finish_result != SQLITE_OK)
      {
        throw_sqlite(_db.get(), finish_result, "finish Raft snapshot restore");
      }
      if (read_raft_state(_db.get()).applied_index != static_cast<std::int64_t>(snapshot.log_index))
      {
        throw std::runtime_error{"restored Raft snapshot has an unexpected applied index"};
      }
    }
    record_raft_snapshot(snapshot);
  }

  std::int64_t SqliteStorage::journal_floor(const std::string_view application_name,
                                            const std::string_view key) const
  {
    if (!_raft_mode)
    {
      return 0;
    }
    const std::lock_guard lock{_mutex};
    const Statement query{
      _db.get(), "SELECT floor FROM journal_retention WHERE application_name = ?1 AND key = ?2"
    };
    bind_key(query, application_name, key);
    return query.next() ? query.integer(0) : 0;
  }

  std::int64_t SqliteStorage::journal_watermark() const
  {
    const std::lock_guard lock{_mutex};
    if (_raft_mode)
    {
      return read_raft_state(_db.get()).journal_watermark;
    }
    const Statement query{_db.get(), "SELECT COALESCE(MAX(sequence), 0) FROM delivery_log"};
    return query.next() ? query.integer(0) : 0;
  }
}
