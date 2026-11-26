#pragma once
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>

#include "entity/entity_message.hpp"

namespace cipple::storage
{
  class SqliteStorage;
}

namespace cipple::tcp
{
  class Session;
}

namespace cipple::server
{
  using ConfigKey = std::pair<std::string, std::string>;

  // All routing, delivery and ACK state belongs to the server's I/O thread.
  class Subscriptions final
  {
  public:
    explicit Subscriptions(storage::SqliteStorage& reader) noexcept;

    void attach(std::uint64_t id, std::shared_ptr<tcp::Session> session);
    void detach(std::uint64_t id) noexcept;
    void stop() noexcept;
    void subscribe(std::uint64_t id, ConfigKey key, std::optional<std::int64_t> cursor);
    void unsubscribe(std::uint64_t id, const ConfigKey& key) noexcept;
    void acknowledge(std::uint64_t id, const Uuid& reply_id, bool success);

    void wake() noexcept;
    bool has_ready() const noexcept;
    void replay_ready() noexcept;
    void expire() noexcept;

  private:
    using Clock = std::chrono::steady_clock;
    using ConnectionId = std::uint64_t;
    using Recipients = std::set<ConnectionId>;

    struct PendingDelivery
    {
      Uuid reply_id;
      std::int64_t sequence;
      Clock::time_point deadline;
    };

    struct Subscription
    {
      std::int64_t cursor;
      bool durable;
      std::optional<PendingDelivery> pending;
    };

    struct Connection
    {
      std::shared_ptr<tcp::Session> session;
      std::map<ConfigKey, Subscription> subscriptions;
    };

    void disconnect(ConnectionId id) noexcept;
    void enqueue_replay(ConnectionId id, const ConfigKey& key, std::int64_t cursor);
    void cancel_replay(ConnectionId id, const ConfigKey& key, std::int64_t cursor) noexcept;
    std::optional<entity::EntityMessage> delivery_base(const entity::EntityMessage& version) const;
    void replay_group(const ConfigKey& key, std::int64_t cursor, const Recipients& recipients) noexcept;

    storage::SqliteStorage& _reader;
    std::unordered_map<ConnectionId, Connection> _connections;
    // At most one queued replay per subscription. Frames are shared only within a replay group.
    std::map<ConfigKey, std::map<std::int64_t, Recipients>> _ready;
  };
}
