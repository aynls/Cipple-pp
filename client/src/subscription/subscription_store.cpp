#include "subscription_store.hpp"

#include <sqlite3.h>

#include <memory>
#include <span>
#include <stdexcept>
#include <utility>

#include "entity/resolve.hpp"
#include "protocol/subscription/dispatch_request.hpp"
#include "binary/buffer_writer.hpp"

namespace cipple::client
{
  namespace
  {
    using entity::EntityMessage;

    void check(sqlite3* database, const int result)
    {
      if (result != SQLITE_OK)
      {
        throw std::runtime_error{sqlite3_errmsg(database)};
      }
    }

    void execute(sqlite3* database, const char* sql)
    {
      check(database, sqlite3_exec(database, sql, nullptr, nullptr, nullptr));
    }

    class Statement final
    {
    public:
      Statement(sqlite3* database, const char* sql)
        : _database{database}, _statement{nullptr, sqlite3_finalize}
      {
        sqlite3_stmt* statement{nullptr};
        check(database, sqlite3_prepare_v2(database, sql, -1, &statement, nullptr));
        _statement.reset(statement);
      }

      // Recycles the statement after a finished, partial, or failed run.
      // sqlite3_reset returns the previous step's error code; reuse does not
      // depend on it, so both calls are intentionally unchecked.
      void reset() noexcept
      {
        sqlite3_reset(_statement.get());
        sqlite3_clear_bindings(_statement.get());
      }

      void bind_blob(const int index, const std::string_view bytes)
      {
        const auto* data{bytes.empty() ? "" : bytes.data()};
        check(_database, sqlite3_bind_blob64(_statement.get(), index, data, bytes.size(), SQLITE_TRANSIENT));
      }

      void bind_blob(const int index, const std::span<const std::uint8_t> bytes)
      {
        bind_blob(index, {reinterpret_cast<const char*>(bytes.data()), bytes.size()});
      }

      void bind_text(const int index, const std::string_view text)
      {
        const auto* data{text.empty() ? "" : text.data()};
        check(_database, sqlite3_bind_text64(_statement.get(), index, data, text.size(), SQLITE_TRANSIENT,
                                             SQLITE_UTF8));
      }

      void bind_int(const int index, const std::int64_t value)
      {
        check(_database, sqlite3_bind_int64(_statement.get(), index, value));
      }

      void bind_null(const int index)
      {
        check(_database, sqlite3_bind_null(_statement.get(), index));
      }

      bool step()
      {
        const auto result{sqlite3_step(_statement.get())};
        if (result == SQLITE_ROW)
        {
          return true;
        }
        if (result != SQLITE_DONE)
        {
          check(_database, result);
        }
        return false;
      }

      std::string_view bytes(const int column) const noexcept
      {
        const auto* data{static_cast<const char*>(sqlite3_column_blob(_statement.get(), column))};
        const auto length{sqlite3_column_bytes(_statement.get(), column)};
        return length == 0 ? std::string_view{} : std::string_view{data, static_cast<std::size_t>(length)};
      }

      std::int64_t integer(const int column) const noexcept
      {
        return sqlite3_column_int64(_statement.get(), column);
      }

    private:
      sqlite3* _database;
      std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> _statement;
    };

    class ResetStatement final
    {
    public:
      explicit ResetStatement(Statement& statement) noexcept : _statement{statement}
      {
      }

      ~ResetStatement() noexcept
      {
        _statement.reset();
      }

      ResetStatement(const ResetStatement&) = delete;
      ResetStatement& operator=(const ResetStatement&) = delete;

    private:
      Statement& _statement;
    };

    std::optional<std::string> find_base(Statement& lookup, const EntityMessage& version)
    {
      const auto* increment{std::get_if<EntityMessage::IncrementalUpdate>(&version.payload())};
      if (!increment)
      {
        return std::nullopt;
      }
      const ResetStatement reset{lookup};
      lookup.bind_blob(1, increment->base_message_uuid.bytes());
      lookup.bind_blob(2, version.application_name());
      lookup.bind_blob(3, version.key());
      if (lookup.step())
      {
        return std::string{lookup.bytes(0)};
      }
      return std::nullopt;
    }

    bool already_stored(Statement& lookup, const message::DispatchRequest& dispatch, const std::string_view encoded)
    {
      const ResetStatement reset{lookup};
      lookup.bind_blob(1, dispatch.entity().uuid().bytes());
      if (!lookup.step())
      {
        return false;
      }
      if (lookup.bytes(0) != encoded)
      {
        throw std::runtime_error{"event UUID already identifies a different version"};
      }
      if (dispatch.base())
      {
        const auto value{materialize_delivery(dispatch)};
        if (!value || *value != lookup.bytes(1))
        {
          throw std::runtime_error{"replayed incremental delivery has a different resolved value"};
        }
      }
      return true;
    }

    void store_event(Statement& insert, const std::string_view endpoint, const message::DispatchRequest& dispatch,
                     const std::string_view encoded, const std::optional<std::string>& value)
    {
      const auto& version{dispatch.entity()};
      const ResetStatement reset{insert};
      insert.bind_blob(1, version.uuid().bytes());
      insert.bind_blob(2, version.application_name());
      insert.bind_blob(3, version.key());
      insert.bind_int(4, version.last_update());
      insert.bind_int(5, version.last_update_server_id());
      insert.bind_text(6, entity::operation_type_wire_name(version.operation_type()));
      insert.bind_blob(7, encoded);
      if (value)
      {
        insert.bind_blob(8, *value);
      }
      else
      {
        insert.bind_null(8);
      }
      insert.bind_text(9, endpoint);
      insert.bind_int(10, dispatch.sequence());
      insert.step();
    }

    bool is_current(Statement& latest, const EntityMessage& version)
    {
      const ResetStatement reset{latest};
      latest.bind_blob(1, version.application_name());
      latest.bind_blob(2, version.key());
      const auto& uuid{version.uuid().bytes()};
      const std::string_view version_uuid{reinterpret_cast<const char*>(uuid.data()), uuid.size()};
      return latest.step() && latest.bytes(0) == version_uuid;
    }

    void advance_cursor(Statement& advance, const EntityMessage& version, const std::int64_t sequence)
    {
      const ResetStatement reset{advance};
      advance.bind_blob(1, version.application_name());
      advance.bind_blob(2, version.key());
      advance.bind_int(3, sequence);
      advance.step();
    }
  }

  struct SubscriptionStore::Statements
  {
    Statement find_event;
    Statement find_base;
    Statement insert_event;
    Statement latest;
    Statement advance;
    Statement lookup_cursor;

    explicit Statements(sqlite3* database)
      : find_event{database, "SELECT encoded, value FROM client_event WHERE uuid = ?1;"}
        , find_base{
          database, R"sql(
          SELECT value FROM client_event
          WHERE uuid = ?1 AND application = ?2 AND key = ?3 AND operation_type = 'update';
        )sql"
        }
        , insert_event{
          database, R"sql(
          INSERT INTO client_event
            (uuid, application, key, last_update, last_update_server_id, operation_type, encoded, value,
             first_endpoint, first_sequence)
          VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10);
        )sql"
        }
        , latest{
          database, R"sql(
          SELECT uuid FROM client_event WHERE application = ?1 AND key = ?2
          ORDER BY last_update DESC, last_update_server_id DESC, uuid DESC LIMIT 1;
        )sql"
        }
        , advance{
          database, R"sql(
          INSERT INTO client_cluster_cursor(application, key, sequence) VALUES (?1, ?2, ?3)
          ON CONFLICT(application, key) DO UPDATE SET sequence = excluded.sequence
          WHERE excluded.sequence > client_cluster_cursor.sequence;
        )sql"
        }
        , lookup_cursor{
          database,
          "SELECT sequence FROM client_cluster_cursor WHERE application=?1 AND key=?2;"
        }
    {
    }
  };

  std::optional<std::string> materialize_delivery(const message::DispatchRequest& dispatch,
                                                  std::optional<std::string_view> saved_base)
  {
    const auto& version{dispatch.entity()};
    if (const auto* increment{std::get_if<EntityMessage::IncrementalUpdate>(&version.payload())})
    {
      const auto& base{*dispatch.base()};
      const auto* full{std::get_if<EntityMessage::Update>(&base.payload())};
      if (!full || base.uuid() != increment->base_message_uuid || base.application_name() != version.application_name()
        || base.key() != version.key())
      {
        throw std::runtime_error{"incremental delivery has an invalid full base"};
      }
      if (saved_base && *saved_base != full->value)
      {
        throw std::runtime_error{"incremental delivery disagrees with the persisted full base"};
      }
      return entity::apply_increment(full->value, *increment);
    }
    if (const auto* update{std::get_if<EntityMessage::Update>(&version.payload())})
    {
      return update->value;
    }
    return std::nullopt;
  }

  SubscriptionStore::SubscriptionStore(const std::string& path)
  {
    if (path.empty() || path == ":memory:")
    {
      throw std::invalid_argument{"--state requires a database file"};
    }
    sqlite3* database{nullptr};
    const auto result{sqlite3_open_v2(path.c_str(), &database, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr)};
    std::unique_ptr<sqlite3, decltype(&sqlite3_close)> db{database, sqlite3_close};
    check(database, result);
    check(database, sqlite3_busy_timeout(database, 5000));
    execute(database, "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;");
    execute(database, R"sql(
      CREATE TABLE IF NOT EXISTS client_event (
        uuid BLOB NOT NULL PRIMARY KEY CHECK (length(uuid) = 16),
        application BLOB NOT NULL,
        key BLOB NOT NULL,
        last_update INTEGER NOT NULL,
        last_update_server_id INTEGER NOT NULL,
        operation_type TEXT NOT NULL,
        encoded BLOB NOT NULL,
        value BLOB,
        first_endpoint TEXT NOT NULL,
        first_sequence INTEGER NOT NULL CHECK (first_sequence > 0)
      ) WITHOUT ROWID;
      CREATE INDEX IF NOT EXISTS client_event_by_version
        ON client_event(application, key, last_update DESC, last_update_server_id DESC, uuid DESC);
      CREATE TABLE IF NOT EXISTS client_cursor (
        endpoint TEXT NOT NULL,
        application BLOB NOT NULL,
        key BLOB NOT NULL,
        sequence INTEGER NOT NULL CHECK (sequence > 0),
        PRIMARY KEY(endpoint, application, key)
      ) WITHOUT ROWID;
      CREATE TABLE IF NOT EXISTS client_cluster_cursor (
        application BLOB NOT NULL,
        key BLOB NOT NULL,
        sequence INTEGER NOT NULL CHECK (sequence > 0),
        PRIMARY KEY(application, key)
      ) WITHOUT ROWID;
    )sql");
    auto statements{std::make_unique<Statements>(database)};
    _database = db.release();
    _statements = std::move(statements);
  }

  SubscriptionStore::~SubscriptionStore() noexcept
  {
    _statements.reset();
    sqlite3_close(_database);
  }

  std::int64_t SubscriptionStore::cursor(const std::string_view, const std::string_view application,
                                         const std::string_view key) const
  {
    auto& lookup{_statements->lookup_cursor};
    const ResetStatement reset{lookup};
    lookup.bind_blob(1, application);
    lookup.bind_blob(2, key);
    return lookup.step() ? lookup.integer(0) : 0;
  }

  AcceptedDelivery SubscriptionStore::accept(const std::string_view endpoint, const message::DispatchRequest& dispatch)
  {
    const auto& version{dispatch.entity()};
    if (dispatch.sequence() <= 0)
    {
      throw std::runtime_error{"durable delivery has no replay sequence"};
    }
    tcp::BufferWriter encoded;
    version.encode(encoded);
    const std::string_view bytes{reinterpret_cast<const char*>(encoded.bytes().data()), encoded.size()};

    execute(_database, "BEGIN IMMEDIATE;");
    try
    {
      const bool first_seen{!already_stored(_statements->find_event, dispatch, bytes)};
      std::optional<std::string> value;
      bool current{false};
      if (first_seen)
      {
        const auto base{find_base(_statements->find_base, version)};
        value = materialize_delivery(dispatch, base);
        store_event(_statements->insert_event, endpoint, dispatch, bytes, value);
        current = is_current(_statements->latest, version);
      }
      advance_cursor(_statements->advance, version, dispatch.sequence());
      execute(_database, "COMMIT;");
      return {first_seen, current, std::move(value)};
    }
    catch (...)
    {
      sqlite3_exec(_database, "ROLLBACK;", nullptr, nullptr, nullptr);
      throw;
    }
  }
}
