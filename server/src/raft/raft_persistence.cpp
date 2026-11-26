#include "raft/raft_persistence.hpp"

#include <libnuraft/buffer.hxx>
#include <libnuraft/cluster_config.hxx>
#include <libnuraft/log_entry.hxx>
#include <libnuraft/srv_config.hxx>
#include <libnuraft/srv_state.hxx>

#include <boost/asio/ip/address_v4.hpp>
#include <sqlite3.h>

#include <algorithm>
#include <cctype>
#include <cstring>
#include <exception>
#include <filesystem>
#include <iostream>
#include <limits>
#include <optional>
#include <set>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace cipple::raft
{
  namespace
  {
    constexpr char kSchema[]{
      R"sql(
      CREATE TABLE IF NOT EXISTS raft_meta (
        key TEXT PRIMARY KEY,
        value BLOB NOT NULL
      ) WITHOUT ROWID;

      CREATE TABLE IF NOT EXISTS raft_member (
        ordinal INTEGER PRIMARY KEY,
        node_id INTEGER NOT NULL UNIQUE,
        endpoint TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS raft_log (
        idx INTEGER PRIMARY KEY,
        term INTEGER NOT NULL,
        value_type INTEGER NOT NULL,
        timestamp_us INTEGER NOT NULL,
        has_crc32 INTEGER NOT NULL CHECK (has_crc32 IN (0, 1)),
        crc32 INTEGER NOT NULL,
        payload BLOB NOT NULL
      );
    )sql"
    };

    constexpr std::string_view kFormatVersionKey{"format_version"};
    constexpr std::string_view kStartIndexKey{"start_index"};
    constexpr std::string_view kBoundaryTermKey{"boundary_term"};
    constexpr std::string_view kNodeIdKey{"node_id"};
    constexpr std::string_view kServerStateKey{"server_state"};
    constexpr std::string_view kClusterConfigKey{"cluster_config"};
    constexpr std::int64_t kFormatVersion{1};

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
        check(db, sqlite3_prepare_v2(db, sql, -1, &_stmt, nullptr), "prepare Raft statement");
      }

      ~Statement() noexcept
      {
        sqlite3_finalize(_stmt);
      }

      Statement(const Statement&) = delete;
      Statement& operator=(const Statement&) = delete;

      void bind(const int index, const std::int64_t value) const
      {
        check(db(), sqlite3_bind_int64(_stmt, index, value), "bind Raft integer");
      }

      void bind_text(const int index, const std::string_view value) const
      {
        const auto* data{value.empty() ? "" : value.data()};
        check(db(), sqlite3_bind_text64(_stmt, index, data, value.size(), SQLITE_TRANSIENT, SQLITE_UTF8),
              "bind Raft text");
      }

      void bind_blob(const int index, const void* data, const std::size_t size) const
      {
        check(db(), sqlite3_bind_blob64(_stmt, index, data ? data : "", size, SQLITE_TRANSIENT), "bind Raft bytes");
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
          throw_sqlite(db(), result, "step Raft statement");
        }
        return false;
      }

      void execute() const
      {
        if (next())
        {
          throw std::runtime_error{"SQLite Raft write unexpectedly returned a row"};
        }
      }

      void reset() const
      {
        check(db(), sqlite3_reset(_stmt), "reset Raft statement");
      }

      bool is_null(const int column) const noexcept
      {
        return sqlite3_column_type(_stmt, column) == SQLITE_NULL;
      }

      int type(const int column) const noexcept
      {
        return sqlite3_column_type(_stmt, column);
      }

      std::int64_t integer(const int column) const noexcept
      {
        return sqlite3_column_int64(_stmt, column);
      }

      std::string_view bytes(const int column) const
      {
        const auto size{sqlite3_column_bytes(_stmt, column)};
        if (size == 0)
        {
          return {};
        }
        const auto* data{static_cast<const char*>(sqlite3_column_blob(_stmt, column))};
        if (data == nullptr)
        {
          throw std::runtime_error{"Invalid SQLite Raft byte column"};
        }
        return {data, static_cast<std::size_t>(size)};
      }

      std::string text(const int column) const
      {
        const auto value{bytes(column)};
        return {value.data(), value.size()};
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
        execute_sql(_db, "BEGIN IMMEDIATE", "begin Raft transaction");
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
        execute_sql(_db, "COMMIT", "commit Raft transaction");
      }

    private:
      sqlite3* _db;
    };

    std::int64_t checked_i64(const nuraft::ulong value, const char* name)
    {
      if (value > static_cast<nuraft::ulong>(std::numeric_limits<std::int64_t>::max()))
      {
        throw std::overflow_error{std::string{"Raft "} + name + " exceeds SQLite integer range"};
      }
      return static_cast<std::int64_t>(value);
    }

    nuraft::ulong checked_ulong(const std::int64_t value, const char* name)
    {
      if (value < 0)
      {
        throw std::runtime_error{std::string{"Negative Raft "} + name + " in SQLite database"};
      }
      return static_cast<nuraft::ulong>(value);
    }

    struct ParsedEndpoint
    {
      std::string host;
      std::uint16_t port{0};
      std::string endpoint;
    };

    ParsedEndpoint parse_ipv4_endpoint(const std::string& endpoint, const bool allow_zero_port)
    {
      const auto colon{endpoint.rfind(':')};
      if (colon == std::string::npos || colon == 0 || colon + 1 == endpoint.size())
      {
        throw std::invalid_argument{"Raft endpoint must have numeric IPv4 host and port: " + endpoint};
      }
      const std::string host{endpoint.substr(0, colon)};
      if (host.find(':') != std::string::npos)
      {
        throw std::invalid_argument{"Raft Windows listener supports numeric IPv4 endpoints only: " + endpoint};
      }
      const std::string port_text{endpoint.substr(colon + 1)};
      if (!std::all_of(port_text.begin(), port_text.end(), [](const unsigned char ch) {
            return std::isdigit(ch) != 0;
          }))
      {
        throw std::invalid_argument{"Raft endpoint port is not numeric: " + endpoint};
      }
      std::uint64_t port{0};
      for (const char ch : port_text)
      {
        port = port * 10 + static_cast<std::uint64_t>(ch - '0');
        if (port > 65535)
        {
          throw std::invalid_argument{"Raft endpoint port is outside 0-65535: " + endpoint};
        }
      }
      if (port == 0 && !allow_zero_port)
      {
        throw std::invalid_argument{"Raft port 0 is allowed only in a single-node configuration: " + endpoint};
      }

      boost::system::error_code error;
      const auto address{boost::asio::ip::make_address_v4(host, error)};
      if (error)
      {
        throw std::invalid_argument{"Raft endpoint host is not numeric IPv4: " + endpoint};
      }

      ParsedEndpoint parsed;
      parsed.host = address.to_string();
      parsed.port = static_cast<std::uint16_t>(port);
      parsed.endpoint = parsed.host + ':' + std::to_string(parsed.port);
      return parsed;
    }

    struct CanonicalMembers
    {
      std::vector<RaftMember> members;
      std::string self_endpoint;
      std::uint16_t self_port{0};
    };

    CanonicalMembers canonicalize_members(const RaftOptions& options)
    {
      if (options.node_id <= 0)
      {
        throw std::invalid_argument{"Raft node id must be positive"};
      }
      if (options.members.empty())
      {
        throw std::invalid_argument{"Raft fixed membership must not be empty"};
      }

      CanonicalMembers result;
      const bool allow_zero_port{options.members.size() == 1};
      std::set<std::int32_t> ids;
      std::set<std::string> endpoints;
      bool found_self{false};
      for (const auto& member : options.members)
      {
        if (member.id <= 0)
        {
          throw std::invalid_argument{"Raft member id must be positive"};
        }
        if (!ids.insert(member.id).second)
        {
          throw std::invalid_argument{"Duplicate Raft member id " + std::to_string(member.id)};
        }
        const auto parsed{parse_ipv4_endpoint(member.endpoint, allow_zero_port)};
        if (!endpoints.insert(parsed.endpoint).second)
        {
          throw std::invalid_argument{"Duplicate Raft member endpoint " + parsed.endpoint};
        }
        result.members.push_back({member.id, parsed.endpoint});
        if (member.id == options.node_id)
        {
          found_self = true;
          result.self_endpoint = parsed.endpoint;
          result.self_port = parsed.port;
        }
      }
      if (!found_self)
      {
        throw std::invalid_argument{"Raft fixed membership does not contain node " + std::to_string(options.node_id)};
      }
      std::sort(result.members.begin(), result.members.end(), [](const RaftMember& left, const RaftMember& right) {
        return left.id < right.id;
      });
      return result;
    }

    void put_meta_integer(sqlite3* db, const std::string_view key, const std::int64_t value)
    {
      const Statement statement{
        db, R"sql(
        INSERT INTO raft_meta(key, value) VALUES (?1, ?2)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
      )sql"
      };
      statement.bind_text(1, key);
      statement.bind(2, value);
      statement.execute();
    }

    void put_meta_blob(sqlite3* db, const std::string_view key, const nuraft::buffer& value)
    {
      const Statement statement{
        db, R"sql(
        INSERT INTO raft_meta(key, value) VALUES (?1, ?2)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
      )sql"
      };
      statement.bind_text(1, key);
      statement.bind_blob(2, value.data_begin(), value.size());
      statement.execute();
    }

    std::optional<std::int64_t> read_meta_integer(sqlite3* db, const std::string_view key)
    {
      const Statement statement{db, "SELECT value FROM raft_meta WHERE key = ?1"};
      statement.bind_text(1, key);
      if (!statement.next())
      {
        return std::nullopt;
      }
      if (statement.type(0) != SQLITE_INTEGER)
      {
        throw std::runtime_error{"Raft SQLite metadata has non-integer value for " + std::string{key}};
      }
      return statement.integer(0);
    }

    std::optional<std::vector<std::uint8_t>> read_meta_blob(sqlite3* db, const std::string_view key)
    {
      const Statement statement{db, "SELECT value FROM raft_meta WHERE key = ?1"};
      statement.bind_text(1, key);
      if (!statement.next())
      {
        return std::nullopt;
      }
      if (statement.type(0) != SQLITE_BLOB)
      {
        throw std::runtime_error{"Raft SQLite metadata has non-blob value for " + std::string{key}};
      }
      const auto value{statement.bytes(0)};
      if (value.empty())
      {
        return std::vector<std::uint8_t>{};
      }
      return std::vector<std::uint8_t>{reinterpret_cast<const std::uint8_t*>(value.data()),
                                       reinterpret_cast<const std::uint8_t*>(value.data()) + value.size()};
    }

    nuraft::ulong read_start_index(sqlite3* db)
    {
      const auto value{read_meta_integer(db, kStartIndexKey)};
      if (!value || *value < 1)
      {
        throw std::runtime_error{"Raft SQLite database has no valid log start index"};
      }
      return static_cast<nuraft::ulong>(*value);
    }

    void write_members(sqlite3* db, const std::vector<RaftMember>& members)
    {
      execute_sql(db, "DELETE FROM raft_member", "clear Raft members");
      const Statement insert{db, "INSERT INTO raft_member(ordinal, node_id, endpoint) VALUES (?1, ?2, ?3)"};
      for (std::size_t index = 0; index < members.size(); ++index)
      {
        insert.bind(1, static_cast<std::int64_t>(index));
        insert.bind(2, members[index].id);
        insert.bind_text(3, members[index].endpoint);
        insert.execute();
        insert.reset();
      }
    }

    std::vector<RaftMember> read_members(sqlite3* db)
    {
      const Statement query{db, "SELECT ordinal, node_id, endpoint FROM raft_member ORDER BY ordinal"};
      std::vector<RaftMember> members;
      while (query.next())
      {
        const auto ordinal{query.integer(0)};
        if (ordinal != static_cast<std::int64_t>(members.size()))
        {
          throw std::runtime_error{"Raft SQLite member ordinals are not contiguous"};
        }
        const auto node_id{query.integer(1)};
        if (node_id <= 0 || node_id > std::numeric_limits<std::int32_t>::max())
        {
          throw std::runtime_error{"Raft SQLite member id is outside the valid range"};
        }
        members.push_back({static_cast<std::int32_t>(node_id), query.text(2)});
      }
      return members;
    }

    bool same_members(const std::vector<RaftMember>& left, const std::vector<RaftMember>& right)
    {
      return left.size() == right.size()
          && std::equal(left.begin(), left.end(), right.begin(), [](const RaftMember& a, const RaftMember& b) {
               return a.id == b.id && a.endpoint == b.endpoint;
             });
    }

    nuraft::ptr<nuraft::buffer> buffer_from_bytes(const std::vector<std::uint8_t>& bytes)
    {
      auto buffer{nuraft::buffer::alloc(bytes.size())};
      if (!bytes.empty())
      {
        buffer->put_raw(bytes.data(), bytes.size());
      }
      buffer->pos(0);
      return buffer;
    }

    nuraft::ptr<nuraft::cluster_config> normalize_config(const nuraft::cluster_config& config)
    {
      auto serialized{config.serialize()};
      auto copy{nuraft::cluster_config::deserialize(*serialized)};
      copy->get_servers().sort([](const nuraft::ptr<nuraft::srv_config>& left,
                                  const nuraft::ptr<nuraft::srv_config>& right) {
        return left->get_id() < right->get_id();
      });
      return copy;
    }

    void validate_fixed_config(const nuraft::cluster_config& config, const std::vector<RaftMember>& members)
    {
      if (config.is_async_replication())
      {
        throw std::runtime_error{"Raft fixed membership rejects asynchronous replication configuration"};
      }
      const auto& servers{config.get_servers()};
      if (servers.size() != members.size())
      {
        throw std::runtime_error{"Raft cluster config membership count differs from the persisted fixed membership"};
      }
      auto member{members.begin()};
      for (const auto& server : servers)
      {
        if (!server)
        {
          throw std::runtime_error{"Raft cluster config contains a null server"};
        }
        if (member == members.end() || server->get_id() != member->id || server->get_endpoint() != member->endpoint)
        {
          throw std::runtime_error{"Raft cluster config membership differs from the persisted fixed membership"};
        }
        if (server->get_dc_id() != 0 || !server->get_aux().empty() || server->is_learner()
              || server->is_new_joiner() || server->get_priority() != nuraft::srv_config::INIT_PRIORITY)
        {
          throw std::runtime_error{"Raft cluster config membership is not in canonical fixed form"};
        }
        ++member;
      }
    }

    nuraft::ptr<nuraft::cluster_config> deserialize_config(const std::vector<std::uint8_t>& bytes,
                                                           const std::vector<RaftMember>& members)
    {
      auto buffer{buffer_from_bytes(bytes)};
      auto config{nuraft::cluster_config::deserialize(*buffer)};
      auto normalized{normalize_config(*config)};
      validate_fixed_config(*normalized, members);
      return normalized;
    }

    void insert_log_entry(sqlite3* db, const nuraft::ulong index, const nuraft::log_entry& entry, const bool replace)
    {
      const auto payload{entry.get_buf_ptr()};
      if (!payload)
      {
        throw std::invalid_argument{"Cannot persist a Raft log entry with a null payload"};
      }
      const Statement statement{
        db, replace
          ? "INSERT OR REPLACE INTO raft_log(idx, term, value_type, timestamp_us, has_crc32, crc32, payload)"
            " VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
          : "INSERT INTO raft_log(idx, term, value_type, timestamp_us, has_crc32, crc32, payload)"
            " VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
      };
      statement.bind(1, checked_i64(index, "log index"));
      statement.bind(2, checked_i64(entry.get_term(), "term"));
      statement.bind(3, static_cast<std::int64_t>(entry.get_val_type()));
      statement.bind(4, checked_i64(entry.get_timestamp(), "timestamp"));
      statement.bind(5, entry.has_crc32() ? 1 : 0);
      statement.bind(6, static_cast<std::int64_t>(entry.get_crc32()));
      statement.bind_blob(7, payload->data_begin(), payload->size());
      statement.execute();
    }

    nuraft::ptr<nuraft::log_entry> read_log_entry(const Statement& row)
    {
      const auto term{checked_ulong(row.integer(0), "term")};
      const auto type{static_cast<nuraft::log_val_type>(row.integer(1))};
      const auto timestamp{checked_ulong(row.integer(2), "timestamp")};
      const bool has_crc{row.integer(3) != 0};
      const auto crc{static_cast<std::uint32_t>(row.integer(4))};
      const auto payload_bytes{row.bytes(5)};
      auto payload{nuraft::buffer::alloc(payload_bytes.size())};
      if (!payload_bytes.empty())
      {
        payload->put_raw(reinterpret_cast<const nuraft::byte*>(payload_bytes.data()), payload_bytes.size());
      }
      payload->pos(0);
      return nuraft::cs_new<nuraft::log_entry>(term, payload, type, timestamp, has_crc, crc, false);
    }

    nuraft::ulong next_slot_unlocked(sqlite3* db, const nuraft::ulong start_index)
    {
      const Statement query{db, "SELECT MAX(idx) FROM raft_log"};
      if (!query.next() || query.is_null(0))
      {
        return start_index;
      }
      const auto last{checked_ulong(query.integer(0), "log index")};
      return std::max(start_index, last + 1);
    }

    nuraft::ulong term_unlocked(sqlite3* db, const nuraft::ulong index)
    {
      const Statement query{db, "SELECT term FROM raft_log WHERE idx = ?1"};
      query.bind(1, checked_i64(index, "log index"));
      return query.next() ? checked_ulong(query.integer(0), "term") : 0;
    }

    constexpr std::size_t kPackedEntryFixed{sizeof(nuraft::ulong) + sizeof(nuraft::byte)
                                            + sizeof(nuraft::ulong) + sizeof(nuraft::byte) + sizeof(std::int32_t)};

    nuraft::ptr<nuraft::buffer> pack_entry(const nuraft::log_entry& entry)
    {
      const auto payload{entry.get_buf_ptr()};
      if (!payload)
      {
        throw std::invalid_argument{"Cannot pack a Raft log entry with a null payload"};
      }
      const auto size{kPackedEntryFixed + payload->size()};
      auto packed{nuraft::buffer::alloc(size)};
      packed->put(entry.get_term());
      packed->put(static_cast<nuraft::byte>(entry.get_val_type()));
      packed->put(static_cast<nuraft::ulong>(entry.get_timestamp()));
      packed->put(static_cast<nuraft::byte>(entry.has_crc32() ? 1 : 0));
      packed->put(static_cast<std::int32_t>(entry.get_crc32()));
      if (payload->size() != 0)
      {
        packed->put_raw(payload->data_begin(), payload->size());
      }
      packed->pos(0);
      return packed;
    }

    nuraft::ptr<nuraft::log_entry> unpack_entry(nuraft::buffer& packed)
    {
      if (packed.size() - packed.pos() < kPackedEntryFixed)
      {
        throw std::runtime_error{"Malformed packed Raft log entry"};
      }
      const auto term{packed.get_ulong()};
      const auto type{static_cast<nuraft::log_val_type>(packed.get_byte())};
      const auto timestamp{packed.get_ulong()};
      const bool has_crc{packed.get_byte() != 0};
      const auto crc{static_cast<std::uint32_t>(packed.get_int())};
      const auto payload_size{packed.size() - packed.pos()};
      auto payload{nuraft::buffer::alloc(payload_size)};
      if (payload_size != 0)
      {
        payload->put_raw(packed.get_raw(payload_size), payload_size);
      }
      payload->pos(0);
      return nuraft::cs_new<nuraft::log_entry>(term, payload, type, timestamp, has_crc, crc, false);
    }
  }

  class RaftDatabase
  {
  public:
    explicit RaftDatabase(const std::filesystem::path& path)
    {
      if (path.empty())
      {
        throw std::invalid_argument{"Raft database path must not be empty"};
      }
      const auto parent{path.parent_path()};
      if (!parent.empty())
      {
        std::filesystem::create_directories(parent);
      }
      const auto utf8_path{path.u8string()};
      sqlite3* db{nullptr};
      const auto result{
        sqlite3_open_v2(reinterpret_cast<const char*>(utf8_path.c_str()), &db,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nullptr)
      };
      _db.reset(db);
      check(db, result, "open Raft database");
      check(db, sqlite3_busy_timeout(db, 5000), "set Raft busy timeout");
      {
        const Statement journal_mode{db, "PRAGMA journal_mode = WAL"};
        if (!journal_mode.next() || journal_mode.bytes(0) != "wal")
        {
          throw std::runtime_error{"Raft persistence requires SQLite WAL mode"};
        }
      }
      execute_sql(db, "PRAGMA synchronous = FULL", "set Raft synchronous mode");
      {
        const Statement synchronous{db, "PRAGMA synchronous"};
        if (!synchronous.next() || synchronous.integer(0) != 2)
        {
          throw std::runtime_error{"Raft persistence requires SQLite synchronous=FULL"};
        }
      }
      execute_sql(db, kSchema, "initialize Raft schema");
    }

    ~RaftDatabase() noexcept = default;

    RaftDatabase(const RaftDatabase&) = delete;
    RaftDatabase& operator=(const RaftDatabase&) = delete;

    sqlite3* db() const noexcept
    {
      return _db.get();
    }

    std::mutex mutex;

  private:
    struct CloseDatabase
    {
      void operator()(sqlite3* db) const noexcept
      {
        sqlite3_close(db);
      }
    };

    std::unique_ptr<sqlite3, CloseDatabase> _db;
  };

  RaftLogStore::RaftLogStore(std::shared_ptr<RaftDatabase> database)
    : _database{std::move(database)}
      , _start_index{1}
  {
    if (!_database)
    {
      throw std::invalid_argument{"Raft log store needs a database"};
    }
    const std::lock_guard lock{_database->mutex};
    _start_index.store(read_start_index(_database->db()));
    const auto term{read_meta_integer(_database->db(), kBoundaryTermKey)};
    if (!term || (*term <= 0 && _start_index.load() > 1))
    {
      throw std::runtime_error{"Raft log has no durable compaction boundary term"};
    }
    _boundary_term = checked_ulong(*term, "compaction boundary term");
  }

  RaftLogStore::~RaftLogStore() noexcept = default;

  nuraft::ulong RaftLogStore::next_slot() const
  {
    const std::lock_guard lock{_database->mutex};
    return next_slot_unlocked(_database->db(), _start_index.load());
  }

  nuraft::ulong RaftLogStore::start_index() const
  {
    return _start_index.load();
  }

  nuraft::ptr<nuraft::log_entry> RaftLogStore::last_entry() const
  {
    const std::lock_guard lock{_database->mutex};
    const auto last{next_slot_unlocked(_database->db(), _start_index.load()) - 1};
    const Statement query{
      _database->db(), "SELECT term, value_type, timestamp_us, has_crc32, crc32, payload FROM raft_log WHERE idx = ?1"
    };
    query.bind(1, checked_i64(last, "log index"));
    if (!query.next())
    {
      return nuraft::cs_new<nuraft::log_entry>(_boundary_term, nuraft::ptr<nuraft::buffer>{});
    }
    return read_log_entry(query);
  }

  nuraft::ulong RaftLogStore::append(nuraft::ptr<nuraft::log_entry>& entry)
  {
    if (!entry)
    {
      throw std::invalid_argument{"Cannot append a null Raft log entry"};
    }
    const std::lock_guard lock{_database->mutex};
    auto* db{_database->db()};
    const auto index{next_slot_unlocked(db, _start_index.load())};
    const Transaction transaction{db};
    insert_log_entry(db, index, *entry, false);
    transaction.commit();
    return index;
  }

  void RaftLogStore::write_at(const nuraft::ulong index, nuraft::ptr<nuraft::log_entry>& entry)
  {
    if (!entry)
    {
      throw std::invalid_argument{"Cannot overwrite a Raft log entry with null"};
    }
    const std::lock_guard lock{_database->mutex};
    auto* db{_database->db()};
    const auto start{_start_index.load()};
    const auto next{next_slot_unlocked(db, start)};
    if (index < start || index > next)
    {
      throw std::out_of_range{"Raft overwrite index is outside the persistent log range"};
    }
    const Transaction transaction{db};
    const Statement erase{db, "DELETE FROM raft_log WHERE idx >= ?1"};
    erase.bind(1, checked_i64(index, "log index"));
    erase.execute();
    insert_log_entry(db, index, *entry, false);
    transaction.commit();
  }

  void RaftLogStore::end_of_append_batch(const nuraft::ulong start, const nuraft::ulong cnt)
  {
    (void)start;
    (void)cnt;
  }

  nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>> RaftLogStore::log_entries(const nuraft::ulong start,
                                                                                   const nuraft::ulong end)
  {
    return log_entries_ext(start, end, 0);
  }

  nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>> RaftLogStore::log_entries_ext(
      const nuraft::ulong start, const nuraft::ulong end, const nuraft::int64 batch_size_hint_in_bytes)
  {
    if (end < start)
    {
      throw std::invalid_argument{"Raft log range end precedes start"};
    }
    auto result{nuraft::cs_new<std::vector<nuraft::ptr<nuraft::log_entry>>>()};
    if (start == end || batch_size_hint_in_bytes < 0)
    {
      return result;
    }
    const std::lock_guard lock{_database->mutex};
    if (start < _start_index.load())
    {
      return nullptr;
    }
    const Statement query{
      _database->db(), R"sql(
      SELECT term, value_type, timestamp_us, has_crc32, crc32, payload, idx
      FROM raft_log WHERE idx >= ?1 AND idx < ?2 ORDER BY idx
    )sql"
    };
    query.bind(1, checked_i64(start, "log index"));
    query.bind(2, checked_i64(end, "log index"));
    nuraft::ulong expected{start};
    std::size_t accumulated{0};
    while (query.next())
    {
      const auto index{checked_ulong(query.integer(6), "log index")};
      if (index != expected)
      {
        return nullptr;
      }
      auto entry{read_log_entry(query)};
      accumulated += entry->get_buf().size();
      result->push_back(std::move(entry));
      ++expected;
      if (batch_size_hint_in_bytes > 0
            && accumulated >= static_cast<std::size_t>(batch_size_hint_in_bytes))
      {
        break;
      }
    }
    if (expected != end
          && (batch_size_hint_in_bytes == 0
              || accumulated < static_cast<std::size_t>(batch_size_hint_in_bytes)))
    {
      return nullptr;
    }
    if (result->empty())
    {
      return nullptr;
    }
    return result;
  }

  nuraft::ptr<nuraft::log_entry> RaftLogStore::entry_at(const nuraft::ulong index)
  {
    const std::lock_guard lock{_database->mutex};
    const Statement query{
      _database->db(), "SELECT term, value_type, timestamp_us, has_crc32, crc32, payload FROM raft_log WHERE idx = ?1"
    };
    query.bind(1, checked_i64(index, "log index"));
    if (!query.next())
    {
      return nullptr;
    }
    return read_log_entry(query);
  }

  nuraft::ulong RaftLogStore::term_at(const nuraft::ulong index)
  {
    if (index < _start_index.load())
    {
      return 0;
    }
    const std::lock_guard lock{_database->mutex};
    const Statement query{_database->db(), "SELECT term FROM raft_log WHERE idx = ?1"};
    query.bind(1, checked_i64(index, "log index"));
    if (!query.next())
    {
      return 0;
    }
    return checked_ulong(query.integer(0), "term");
  }

  nuraft::ptr<nuraft::buffer> RaftLogStore::pack(const nuraft::ulong index, const nuraft::int32 cnt)
  {
    if (cnt < 0)
    {
      throw std::invalid_argument{"Raft pack count must not be negative"};
    }
    const std::lock_guard lock{_database->mutex};
    const Statement query{
      _database->db(), R"sql(
      SELECT term, value_type, timestamp_us, has_crc32, crc32, payload, idx
      FROM raft_log WHERE idx >= ?1 ORDER BY idx LIMIT ?2
    )sql"
    };
    query.bind(1, checked_i64(index, "log index"));
    query.bind(2, cnt);

    std::vector<nuraft::ptr<nuraft::buffer>> packed_entries;
    std::size_t total_size{sizeof(std::int32_t) + static_cast<std::size_t>(cnt) * sizeof(std::int32_t)};
    nuraft::ulong expected{index};
    while (query.next())
    {
      const auto row_index{checked_ulong(query.integer(6), "log index")};
      if (row_index != expected)
      {
        throw std::runtime_error{"Cannot pack a non-contiguous Raft log range"};
      }
      auto packed{pack_entry(*read_log_entry(query))};
      if (total_size > std::numeric_limits<std::size_t>::max() - packed->size())
      {
        throw std::overflow_error{"Packed Raft log is too large"};
      }
      total_size += packed->size();
      packed_entries.push_back(std::move(packed));
      ++expected;
    }
    if (packed_entries.size() != static_cast<std::size_t>(cnt))
    {
      throw std::runtime_error{"Cannot pack missing Raft log entries"};
    }

    auto output{nuraft::buffer::alloc(total_size)};
    output->put(cnt);
    for (const auto& packed : packed_entries)
    {
      output->put(static_cast<std::int32_t>(packed->size()));
      output->put(*packed);
    }
    output->pos(0);
    return output;
  }

  void RaftLogStore::apply_pack(const nuraft::ulong index, nuraft::buffer& pack)
  {
    pack.pos(0);
    if (pack.size() < sizeof(std::int32_t))
    {
      throw std::runtime_error{"Malformed Raft log pack"};
    }
    const auto count{pack.get_int()};
    if (count < 0)
    {
      throw std::runtime_error{"Raft log pack has a negative entry count"};
    }
    std::vector<nuraft::ptr<nuraft::log_entry>> entries;
    entries.reserve(static_cast<std::size_t>(count));
    for (std::int32_t item = 0; item < count; ++item)
    {
      if (pack.size() - pack.pos() < sizeof(std::int32_t))
      {
        throw std::runtime_error{"Truncated Raft log pack entry header"};
      }
      const auto size{pack.get_int()};
      if (size < static_cast<std::int32_t>(kPackedEntryFixed)
            || static_cast<std::size_t>(size) > pack.size() - pack.pos())
      {
        throw std::runtime_error{"Truncated Raft log pack entry"};
      }
      auto wire{nuraft::buffer::alloc(static_cast<std::size_t>(size))};
      pack.get(wire);
      wire->pos(0);
      entries.push_back(unpack_entry(*wire));
    }
    if (index == 0 || index + entries.size() < index)
    {
      throw std::invalid_argument{"Invalid Raft log pack start index"};
    }

    const std::lock_guard lock{_database->mutex};
    auto* db{_database->db()};
    const Transaction transaction{db};
    for (std::size_t offset = 0; offset < entries.size(); ++offset)
    {
      insert_log_entry(db, index + offset, *entries[offset], true);
    }
    transaction.commit();
  }

  bool RaftLogStore::compact(const nuraft::ulong last_log_index)
  {
    if (last_log_index >= static_cast<nuraft::ulong>(std::numeric_limits<std::int64_t>::max()))
    {
      throw std::overflow_error{"Raft compaction index exceeds SQLite integer range"};
    }
    const std::lock_guard lock{_database->mutex};
    auto* db{_database->db()};
    const auto current_start{_start_index.load()};
    const bool installing{_incoming_snapshot && _incoming_snapshot->index == last_log_index};
    if (current_start > last_log_index)
    {
      if (installing)
      {
        if (current_start - 1 == last_log_index && _boundary_term != _incoming_snapshot->term)
        {
          throw std::runtime_error{"incoming Raft snapshot disagrees with the compacted term"};
        }
        _incoming_snapshot.reset();
      }
      return true;
    }
    const auto stored_term{term_unlocked(db, last_log_index)};
    const auto term{installing ? _incoming_snapshot->term : stored_term};
    if (term == 0)
    {
      throw std::runtime_error{"Raft cannot compact a log index without its term"};
    }
    const Transaction transaction{db};
    if (installing && term != stored_term)
    {
      execute_sql(db, "DELETE FROM raft_log", "discard suffix conflicting with installed snapshot");
    }
    else
    {
      const Statement erase{db, "DELETE FROM raft_log WHERE idx <= ?1"};
      erase.bind(1, checked_i64(last_log_index, "log index"));
      erase.execute();
    }
    put_meta_integer(db, kStartIndexKey, static_cast<std::int64_t>(last_log_index + 1));
    put_meta_integer(db, kBoundaryTermKey, checked_i64(term, "compaction boundary term"));
    transaction.commit();
    _boundary_term = term;
    _start_index.store(last_log_index + 1);
    if (installing)
    {
      _incoming_snapshot.reset();
    }
    return true;
  }

  void RaftLogStore::expect_snapshot(const nuraft::ulong index, const nuraft::ulong term)
  {
    if (index == 0 || term == 0)
    {
      throw std::invalid_argument{"Raft snapshot index and term must be positive"};
    }
    const std::lock_guard lock{_database->mutex};
    _incoming_snapshot = SnapshotBoundary{index, term};
  }

  bool RaftLogStore::flush()
  {
    const std::lock_guard lock{_database->mutex};
    return sqlite3_get_autocommit(_database->db()) != 0;
  }

  nuraft::ulong RaftLogStore::last_durable_index()
  {
    return next_slot() - 1;
  }

  RaftStateManager::RaftStateManager(const RaftOptions& options)
    : _node_id{options.node_id}
  {
    const auto canonical{canonicalize_members(options)};
    _members = canonical.members;
    _self_endpoint = canonical.self_endpoint;
    _self_port = canonical.self_port;
    _database = std::make_shared<RaftDatabase>(options.database);

    {
      const std::lock_guard lock{_database->mutex};
      auto* db{_database->db()};
      const Transaction transaction{db};
      const auto version{read_meta_integer(db, kFormatVersionKey)};
      if (!version)
      {
        put_meta_integer(db, kFormatVersionKey, kFormatVersion);
        put_meta_integer(db, kNodeIdKey, _node_id);
        put_meta_integer(db, kStartIndexKey, 1);
        put_meta_integer(db, kBoundaryTermKey, 0);
        write_members(db, _members);
        nuraft::cluster_config initial;
        for (const auto& member : _members)
        {
          initial.get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(member.id, member.endpoint));
        }
        auto serialized{initial.serialize()};
        put_meta_blob(db, kClusterConfigKey, *serialized);
      }
      else
      {
        if (*version != kFormatVersion)
        {
          throw std::runtime_error{"Unsupported Raft SQLite format version " + std::to_string(*version)};
        }
        if (read_meta_integer(db, kNodeIdKey) != _node_id)
        {
          throw std::runtime_error{"Raft database was initialized with a different node id"};
        }
        const auto start{read_start_index(db)};
        if (next_slot_unlocked(db, start) > 1 && !read_meta_blob(db, kServerStateKey))
        {
          throw std::runtime_error{"Raft database has logs but is missing its persisted term and vote"};
        }
        const auto persisted_members{read_members(db)};
        if (!same_members(persisted_members, _members))
        {
          throw std::runtime_error{"Raft database fixed membership or endpoints differ from the requested configuration"};
        }
        const auto stored_config{read_meta_blob(db, kClusterConfigKey)};
        if (!stored_config)
        {
          throw std::runtime_error{"Raft SQLite database has no persisted cluster config"};
        }
        (void)deserialize_config(*stored_config, _members);
      }
      transaction.commit();
    }
    _log_store = nuraft::ptr<RaftLogStore>{new RaftLogStore(_database)};
  }

  RaftStateManager::~RaftStateManager() noexcept = default;

  nuraft::ptr<nuraft::cluster_config> RaftStateManager::load_config()
  {
    const std::lock_guard lock{_database->mutex};
    const auto stored{read_meta_blob(_database->db(), kClusterConfigKey)};
    if (!stored)
    {
      throw std::runtime_error{"Raft SQLite database has no persisted cluster config"};
    }
    return deserialize_config(*stored, _members);
  }

  void RaftStateManager::save_config(const nuraft::cluster_config& config)
  {
    auto normalized{normalize_config(config)};
    validate_fixed_config(*normalized, _members);
    const std::lock_guard lock{_database->mutex};
    auto* db{_database->db()};
    const Transaction transaction{db};
    auto serialized{normalized->serialize()};
    put_meta_blob(db, kClusterConfigKey, *serialized);
    write_members(db, _members);
    transaction.commit();
  }

  void RaftStateManager::save_state(const nuraft::srv_state& state)
  {
    try
    {
      auto serialized{state.serialize()};
      const std::lock_guard lock{_database->mutex};
      auto* db{_database->db()};
      const Transaction transaction{db};
      put_meta_blob(db, kServerStateKey, *serialized);
      transaction.commit();
    }
    catch (const std::exception& error)
    {
      // NuRaft has already changed its in-memory term or vote. Continuing
      // after a failed durable write could grant a second vote after restart.
      std::cerr << "Raft failed to persist its term or vote: " << error.what() << '\n';
      std::terminate();
    }
  }

  nuraft::ptr<nuraft::srv_state> RaftStateManager::read_state()
  {
    const std::lock_guard lock{_database->mutex};
    const auto stored{read_meta_blob(_database->db(), kServerStateKey)};
    if (!stored)
    {
      return nullptr;
    }
    auto buffer{buffer_from_bytes(*stored)};
    return nuraft::srv_state::deserialize(*buffer);
  }

  nuraft::ptr<nuraft::log_store> RaftStateManager::load_log_store()
  {
    return _log_store;
  }

  nuraft::int32 RaftStateManager::server_id()
  {
    return _node_id;
  }

  void RaftStateManager::system_exit(const int exit_code)
  {
    std::cerr << "Raft fatal error " << exit_code << " in node " << _node_id << '\n';
    std::terminate();
  }

  void RaftStateManager::check_config(const nuraft::cluster_config& config) const
  {
    validate_fixed_config(*normalize_config(config), _members);
  }

  const std::string& RaftStateManager::self_endpoint() const noexcept
  {
    return _self_endpoint;
  }

  std::uint16_t RaftStateManager::self_port() const noexcept
  {
    return _self_port;
  }

  const std::vector<RaftMember>& RaftStateManager::members() const noexcept
  {
    return _members;
  }
}
