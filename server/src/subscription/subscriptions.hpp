#pragma once
#include <chrono>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "entity/entity_message.hpp"
#include "protocol/frame_codec.hpp"

namespace cipple::message
{
  class Message;
}

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
  struct RelayRecoveryOptions;

  // All routing, delivery and ACK state belongs to the server's I/O thread.
  class Subscriptions final
  {
  public:
    Subscriptions(storage::SqliteStorage& reader, RelayRecoveryOptions recovery) noexcept;

    void attach(std::uint64_t id, std::shared_ptr<tcp::Session> session, std::string address);
    void advertise(std::uint64_t id, std::uint16_t port, std::uint16_t slots);
    void relay_status(std::uint64_t id, const Uuid& token, bool ready);
    void detach(std::uint64_t id) noexcept;
    void stop() noexcept;
    void subscribe(std::uint64_t id, ConfigKey key, std::optional<std::int64_t> cursor);
    void enable_relay(std::uint64_t id, const ConfigKey& key);
    void unsubscribe(std::uint64_t id, const ConfigKey& key) noexcept;
    void acknowledge(std::uint64_t id, const Uuid& reply_id, bool success);

    void wake() noexcept;
    bool has_ready() const noexcept;
    void replay_ready() noexcept;
    void expire() noexcept;
    std::optional<std::chrono::steady_clock::time_point> recovery_due() const noexcept;

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
      std::uint64_t generation;
      std::uint64_t branch{0};
      Clock::time_point link_after{};
      Clock::time_point captain_after{};
      std::optional<Uuid> link;
      bool retry{false};
      bool queued{false};
      bool waiting{false};
    };

    struct Connection
    {
      std::shared_ptr<tcp::Session> session;
      std::map<ConfigKey, Subscription> subscriptions;
      std::string address;
      bool relay{false};
      std::uint16_t port{0};
      std::uint16_t slots{0};
      std::uint16_t used_slots{0};
    };

    struct Branch
    {
      Recipients members;
      ConnectionId captain{0};
      std::uint64_t epoch{0};
    };

    enum class LinkPhase { kAuthorizing, kConnecting, kReady };

    struct Link
    {
      ConfigKey key;
      ConnectionId captain;
      ConnectionId member;
      LinkPhase phase;
      Clock::time_point deadline;
    };

    struct RoundTarget
    {
      ConnectionId id;
      std::uint64_t generation;
      bool acknowledged{false};
    };

    struct Round
    {
      ConfigKey key;
      std::uint64_t branch;
      std::uint64_t epoch;
      Clock::time_point deadline;
      std::vector<RoundTarget> targets;
    };

    struct RecoveryTask
    {
      ConnectionId id;
      ConfigKey key;
      std::uint64_t generation;
      std::int64_t cursor;
    };

    struct PreparedRecovery
    {
      Uuid attempt;
      std::int64_t sequence;
      tcp::EncodedFrame frame;
    };

    void disconnect(ConnectionId id) noexcept;
    Subscription* find_subscription(ConnectionId id, const ConfigKey& key) noexcept;
    bool send_control(ConnectionId id, const message::Message& message) const noexcept;
    void join_branch(ConnectionId id, const ConfigKey& key, Subscription& subscription);
    void leave_branch(ConnectionId id, const ConfigKey& key, Subscription& subscription);
    void maintain_branch(const ConfigKey& key, Branch& branch, Clock::time_point now);
    bool can_captain(ConnectionId id, const ConfigKey& key, Clock::time_point now) noexcept;
    void retire_captain(const ConfigKey& key, Branch& branch);
    void drop_link(Uuid token, Clock::time_point retry_after = {}) noexcept;
    bool needs_recovery(ConnectionId id, const ConfigKey& key, const Subscription& subscription) const noexcept;
    void enqueue_recovery(ConnectionId id, const ConfigKey& key, Subscription& subscription);
    void recheck_recovery_head(ConnectionId id, const Subscription& subscription) noexcept;
    void refill_recovery_queue();
    void drain_recovery();
    void finish_rounds(Clock::time_point now);
    void enqueue_replay(ConnectionId id, const ConfigKey& key, std::int64_t cursor);
    void cancel_replay(ConnectionId id, const ConfigKey& key, std::int64_t cursor) noexcept;
    std::optional<entity::EntityMessage> delivery_base(const entity::EntityMessage& version) const;
    void replay_group(const ConfigKey& key, std::int64_t cursor, const Recipients& recipients) noexcept;
    void relay_group(const ConfigKey& key, std::int64_t cursor, const Recipients& recipients,
                     const entity::EntityMessage& version, const std::optional<entity::EntityMessage>& base,
                     std::int64_t sequence, const Uuid& reply_id, tcp::EncodedFrame& direct, Recipients& sent);

    storage::SqliteStorage& _reader;
    std::unordered_map<ConnectionId, Connection> _connections;
    std::map<ConfigKey, std::map<std::int64_t, Recipients>> _ready;
    std::map<ConfigKey, std::map<std::uint64_t, Branch>> _branches;
    std::unordered_map<Uuid, Link> _links;
    std::unordered_map<Uuid, Round> _rounds;
    std::uint64_t _next_subscription{0};
    std::uint64_t _next_branch{0};
    bool _stopping{false};
    std::deque<RecoveryTask> _recovery_queue;
    std::size_t _recovery_waiters{0};
    std::optional<PreparedRecovery> _prepared;
    std::optional<Clock::time_point> _recovery_due;
    double _bytes_per_second;
    double _burst_bytes;
    double _tokens;
    Clock::time_point _refill_at;
  };
}
