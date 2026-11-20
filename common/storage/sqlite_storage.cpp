#include "sqlite_storage.hpp"

#include <sqlite3.h>

#include <stdexcept>
#include <unordered_set>
#include <utility>

namespace cipple::storage
{
  namespace
  {
    using entity::EntityMessage;
    using entity::OperationType;

    constexpr char kSchema[] = R"sql(
      CREATE TABLE IF NOT EXISTS item (
        application_name BLOB NOT NULL,
        key BLOB NOT NULL,
        PRIMARY KEY (application_name, key)
      ) WITHOUT ROWID;

      CREATE TABLE IF NOT EXISTS message (
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

      CREATE TABLE IF NOT EXISTS ack (
        message_uuid BLOB NOT NULL REFERENCES message(uuid),
        node_id INTEGER NOT NULL,
        acknowledged INTEGER NOT NULL DEFAULT 0 CHECK (acknowledged IN (0, 1)),
        PRIMARY KEY (message_uuid, node_id)
      ) WITHOUT ROWID;

      CREATE INDEX IF NOT EXISTS message_history
        ON message(application_name, key, last_update, last_update_server_id, uuid)
        WHERE retained = 1;
      CREATE INDEX IF NOT EXISTS ack_pending ON ack(node_id, message_uuid) WHERE acknowledged = 0;

      CREATE TABLE IF NOT EXISTS delivery_log (
        sequence INTEGER PRIMARY KEY,
        message_uuid BLOB NOT NULL UNIQUE REFERENCES message(uuid),
        application_name BLOB NOT NULL,
        key BLOB NOT NULL
      );
      CREATE INDEX IF NOT EXISTS delivery_log_topic ON delivery_log(application_name, key, sequence);
    )sql";

    constexpr std::string_view kPendingForNode = R"sql(
      JOIN ack AS a ON a.message_uuid = m.uuid
      WHERE a.node_id = ?1 AND a.acknowledged = 0 AND m.retained = 1
      ORDER BY m.last_update, m.last_update_server_id, m.uuid
      LIMIT ?2
    )sql";

    [[noreturn]] void throw_sqlite(sqlite3* db, const int error, const char* operation)
    {
      const auto* detail = db ? sqlite3_errmsg(db) : sqlite3_errstr(error);
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
        const auto& bytes = uuid.bytes();
        bind_bytes(index, {reinterpret_cast<const char*>(bytes.data()), bytes.size()});
      }

      void bind_bytes(const int index, const std::string_view value) const
      {
        const auto* data = value.empty() ? "" : value.data();
        check(db(), sqlite3_bind_blob64(_stmt, index, data, value.size(), SQLITE_TRANSIENT), "bind bytes");
      }

      void bind_text(const int index, const std::string_view value) const
      {
        const auto* data = value.empty() ? "" : value.data();
        check(db(), sqlite3_bind_text64(_stmt, index, data, value.size(), SQLITE_TRANSIENT, SQLITE_UTF8),
              "bind text");
      }

      bool next() const
      {
        const auto result = sqlite3_step(_stmt);
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
        const auto* data = static_cast<const char*>(sqlite3_column_blob(_stmt, column));
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
        const auto value = bytes(column);
        if (value.size() != 16)
        {
          throw std::runtime_error{"Invalid UUID in SQLite message row"};
        }
        const std::span<const std::uint8_t, 16> uuid_bytes{
          reinterpret_cast<const std::uint8_t*>(value.data()), value.size()};
        return Uuid::from_bytes(uuid_bytes);
      }

    private:
      sqlite3* db() const noexcept
      {
        return sqlite3_db_handle(_stmt);
      }

      sqlite3_stmt* _stmt = nullptr;
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
      const Statement insert{db, R"sql(
        INSERT INTO item(application_name, key) VALUES (?1, ?2)
        ON CONFLICT(application_name, key) DO NOTHING
      )sql"};
      bind_key(insert, message.application_name(), message.key());
      insert.execute();
    }

    void record_delivery(sqlite3* db, const EntityMessage& message)
    {
      const Statement insert{db, R"sql(
        INSERT INTO delivery_log(application_name, key, message_uuid) VALUES (?1, ?2, ?3)
        ON CONFLICT(message_uuid) DO NOTHING
      )sql"};
      bind_key(insert, message.application_name(), message.key());
      insert.bind(3, message.uuid());
      insert.execute();
    }

    void insert_message(sqlite3* db, const EntityMessage& message)
    {
      const Statement insert{db, R"sql(
        INSERT INTO message(application_name, key, uuid, last_update, last_update_server_id,
                            operation_type, value, base_message_uuid, atomic_operation)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
      )sql"};
      bind_key(insert, message.application_name(), message.key());
      insert.bind(3, message.uuid());
      insert.bind(4, message.last_update());
      insert.bind(5, message.last_update_server_id());
      insert.bind_text(6, entity::operation_type_wire_name(message.operation_type()));
      if (const auto* update = std::get_if<EntityMessage::Update>(&message.payload()))
      {
        insert.bind_bytes(7, update->value);
      }
      else if (const auto* delta = std::get_if<EntityMessage::IncrementalUpdate>(&message.payload()))
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
      const auto* supplied_update =
          supplied_base ? std::get_if<EntityMessage::Update>(&supplied_base->payload()) : nullptr;
      if (base_uuid == message.uuid() ||
          (supplied_base && (!supplied_update || supplied_base->uuid() != base_uuid ||
                             supplied_base->application_name() != message.application_name() ||
                             supplied_base->key() != message.key())))
      {
        return false;
      }

      {
        const Statement query{db, R"sql(
          SELECT application_name, key, operation_type, last_update, last_update_server_id, retained, value
          FROM message WHERE uuid = ?1
        )sql"};
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
      std::string sql = R"sql(
        SELECT m.uuid, m.application_name, m.key, m.last_update, m.last_update_server_id,
               m.operation_type, m.value, m.base_message_uuid, m.atomic_operation, m.retained
      )sql";
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
      const auto operation = entity::operation_type_parse(row.bytes(5));
      if (operation == OperationType::kUpdate)
      {
        payload = EntityMessage::Update{std::string{row.bytes(6)}};
      }
      else if (operation == OperationType::kIncrementalUpdate)
      {
        const auto atomic_operation = entity::atomic_operation_parse(row.bytes(8));
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
      return {row.uuid(0), std::string{row.bytes(1)}, std::string{row.bytes(2)},
              row.integer(3), static_cast<std::int32_t>(row.integer(4)), std::move(payload)};
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
  }

  SqliteStorage::SqliteStorage(const std::filesystem::path& path)
  {
    const auto utf8_path = path.u8string();
    sqlite3* db = nullptr;
    const auto result = sqlite3_open_v2(reinterpret_cast<const char*>(utf8_path.c_str()), &db,
                                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
    _db.reset(db);
    check(db, result, "open database");
    check(db, sqlite3_busy_timeout(db, 5000), "set busy timeout");
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
    const Transaction transaction{db};
    execute_sql(db, kSchema, "initialize storage schema");
    auto schema_version = 0;
    {
      const Statement version{db, "PRAGMA user_version"};
      if (version.next())
      {
        schema_version = static_cast<int>(version.integer(0));
      }
    }
    if (schema_version == 0)
    {
      execute_sql(db, R"sql(
        INSERT INTO delivery_log(message_uuid, application_name, key)
          SELECT uuid, application_name, key FROM message WHERE retained = 1
          ORDER BY last_update, last_update_server_id, uuid;
        PRAGMA user_version = 1;
      )sql", "initialize replay history from existing messages");
    }
    else if (schema_version != 1)
    {
      throw std::runtime_error{"unsupported storage schema version"};
    }
    transaction.commit();
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
    const std::lock_guard lock{_mutex};
    auto* db = _db.get();
    const Transaction transaction{db};
    if (message_exists(db, message.uuid()))
    {
      return AppendResult::kAlreadyExists;
    }

    insert_item(db, message);
    const auto* delta = std::get_if<EntityMessage::IncrementalUpdate>(&message.payload());
    if (delta && !ensure_base(db, message, delta->base_message_uuid, supplied_base))
    {
      return AppendResult::kBaseUnavailable;
    }
    insert_message(db, message);
    if (!target_node_ids.empty())
    {
      const Statement insert_ack{db, R"sql(
        INSERT INTO ack(message_uuid, node_id) VALUES (?1, ?2)
        ON CONFLICT(message_uuid, node_id) DO NOTHING
      )sql"};
      insert_ack.bind(1, message.uuid());
      for (const auto node_id : target_node_ids)
      {
        insert_ack.bind(2, node_id);
        insert_ack.execute();
        insert_ack.reset();
      }
    }
    transaction.commit();
    return AppendResult::kInserted;
  }

  bool SqliteStorage::mark_acked(const Uuid& message_uuid, const std::int32_t node_id)
  {
    const std::lock_guard lock{_mutex};
    const Statement update{_db.get(), R"sql(
      UPDATE ack SET acknowledged = 1 WHERE message_uuid = ?1 AND node_id = ?2 AND acknowledged = 0
    )sql"};
    update.bind(1, message_uuid);
    update.bind(2, node_id);
    update.execute();
    return sqlite3_changes(_db.get()) != 0;
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
    const auto sql = select_messages(R"sql(
      WHERE m.application_name = ?1 AND m.key = ?2 AND m.retained = 1
      ORDER BY m.last_update, m.last_update_server_id, m.uuid
    )sql");
    const Statement query{_db.get(), sql.c_str()};
    bind_key(query, application_name, key);
    return read_messages(query);
  }

  std::optional<EntityMessage> SqliteStorage::latest(const std::string_view application_name,
                                                     const std::string_view key) const
  {
    const std::lock_guard lock{_mutex};
    const auto sql = select_messages(R"sql(
      WHERE m.application_name = ?1 AND m.key = ?2 AND m.retained = 1
      ORDER BY m.last_update DESC, m.last_update_server_id DESC, m.uuid DESC
      LIMIT 1
    )sql");
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
    const auto sql = select_messages("WHERE m.uuid = ?1");
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

  std::vector<EntityMessage> SqliteStorage::pending_for_node(const std::int32_t node_id, const std::int64_t limit) const
  {
    const std::lock_guard lock{_mutex};
    const auto sql = select_messages(kPendingForNode);
    const Statement query{_db.get(), sql.c_str()};
    query.bind(1, node_id);
    query.bind(2, limit == 0 ? -1 : limit);
    return read_messages(query);
  }

  std::vector<Uuid> SqliteStorage::pending_ids_for_node(const std::int32_t node_id, const std::int64_t limit) const
  {
    const std::lock_guard lock{_mutex};
    std::string sql{"SELECT m.uuid FROM message AS m "};
    sql.append(kPendingForNode);
    const Statement query{_db.get(), sql.c_str()};
    query.bind(1, node_id);
    query.bind(2, limit);
    std::vector<Uuid> result;
    while (query.next())
    {
      result.push_back(query.uuid(0));
    }
    return result;
  }

  std::vector<JournalEntry> SqliteStorage::journal_after(const std::string_view application_name,
                                                         const std::string_view key, const std::int64_t after,
                                                         const std::int64_t limit) const
  {
    const std::lock_guard lock{_mutex};
    const auto sql = select_messages(R"sql(
      JOIN delivery_log AS d ON d.message_uuid = m.uuid
      WHERE d.application_name = ?1 AND d.key = ?2 AND d.sequence > ?3 AND m.retained = 1
      ORDER BY d.sequence
      LIMIT ?4
    )sql", true);
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
    auto* db = _db.get();
    const Transaction transaction{db};
    std::unordered_set<Uuid> protected_bases;
    std::vector<Uuid> candidates;
    {
      const Statement query{db, R"sql(
        SELECT m.uuid, m.base_message_uuid,
               EXISTS (SELECT 1 FROM ack AS a WHERE a.message_uuid = m.uuid AND a.acknowledged = 0)
        FROM message AS m
        WHERE m.application_name = ?1 AND m.key = ?2 AND m.retained = 1
        ORDER BY m.last_update DESC, m.last_update_server_id DESC, m.uuid DESC
      )sql"};
      bind_key(query, application_name, key);
      std::size_t rank = 0;
      while (query.next())
      {
        const bool keep = rank < keep_latest || query.integer(2) != 0;
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

    const Statement reclaim{db, R"sql(
      UPDATE message SET retained = 0, value = NULL, base_message_uuid = NULL, atomic_operation = NULL
      WHERE uuid = ?1
    )sql"};
    const Statement clear_ack{db, "DELETE FROM ack WHERE message_uuid = ?1 AND acknowledged = 1"};
    std::size_t reclaimed = 0;
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
}
