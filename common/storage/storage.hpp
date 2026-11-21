#pragma once
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "entity/entity_message.hpp"

namespace cipple::storage
{
  struct ItemKey
  {
    std::string application_name;
    std::string key;
  };

  struct JournalEntry
  {
    std::int64_t sequence;
    entity::EntityMessage message;
  };

  enum class AppendResult
  {
    kInserted,
    kAlreadyExists,
    kBaseUnavailable,
  };

  struct AppendOutcome
  {
    AppendResult result;
    // Evaluated inside the append transaction against GET's version order.
    bool is_current = false;
  };

  enum class MessageUnavailable
  {
    kMissing,
    kRecycled,
  };

  // Exclusive lower bound for a pending scan. Callers pass an empty optional for the
  // first page so versions with a negative HLC or node id stay reachable.
  struct PendingCursor
  {
    std::int64_t last_update = 0;
    std::int32_t last_update_server_id = 0;
    Uuid uuid{};
  };

  struct PendingId
  {
    Uuid uuid{};
    std::int64_t last_update = 0;
    std::int32_t last_update_server_id = 0;
  };

  class Storage
  {
  public:
    virtual ~Storage() noexcept;

    // Commits the message and its targets together. An empty span registers no targets.
    // For an incremental update, supplied_base can restore the original full update in the same transaction.
    // A duplicate UUID leaves the stored message and its ACK targets intact.
    virtual AppendResult append(const entity::EntityMessage& message,
                                std::span<const std::int32_t> target_node_ids,
                                const entity::EntityMessage* supplied_base = nullptr) = 0;

    // Returns the committed result. is_current is meaningful for kInserted.
    virtual AppendOutcome append_with_outcome(const entity::EntityMessage& message,
                                              std::span<const std::int32_t> target_node_ids,
                                              const entity::EntityMessage* supplied_base = nullptr) = 0;

    // Lists all known keys, including keys whose latest version is a deletion.
    virtual std::vector<ItemKey> list_keys() const = 0;

    // Retained messages in ascending order by (last_update, last_update_server_id, uuid).
    virtual std::vector<entity::EntityMessage> history(std::string_view application_name,
                                                       std::string_view key) const = 0;
    // The last retained version, including a deletion if it is the latest.
    virtual std::optional<entity::EntityMessage> latest(std::string_view application_name,
                                                        std::string_view key) const;
    virtual std::expected<entity::EntityMessage, MessageUnavailable> find_message(const Uuid& uuid) const = 0;
    // Page of unacknowledged targets in version order, strictly after `after`.
    // An empty cursor has no lower bound. A non-positive limit returns nothing.
    virtual std::vector<PendingId> pending_ids_after(std::int32_t node_id, const std::optional<PendingCursor>& after,
                                                    std::int64_t limit) const = 0;
    // One synchronous=FULL transaction. Already-acknowledged ids stay acknowledged and are not counted.
    virtual std::size_t mark_acked_batch(std::int32_t node_id, std::span<const Uuid> message_uuids) = 0;

    // Replica-local append order, independent of the version's conflict order.
    virtual std::vector<JournalEntry> journal_after(std::string_view application_name, std::string_view key,
                                                   std::int64_t after, std::int64_t limit) const = 0;
    virtual std::int64_t journal_sequence(const Uuid& uuid) const = 0;
    virtual std::int64_t version_watermark() const = 0;

    // Reclaims payloads and settled acknowledgements while preserving UUIDs for deduplication.
    // keep_latest must be positive. Returns the number of messages reclaimed for this key.
    virtual std::size_t recycle(std::string_view application_name, std::string_view key,
                                std::size_t keep_latest) = 0;
  };
}
