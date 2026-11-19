#pragma once
#include <cstddef>
#include <cstdint>
#include <expected>
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

  enum class AppendResult
  {
    kInserted,
    kAlreadyExists,
    kBaseUnavailable,
  };

  enum class MessageUnavailable
  {
    kMissing,
    kRecycled,
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

    // Returns true only when a registered target transitions to acknowledged.
    virtual bool mark_acked(const Uuid& message_uuid, std::int32_t node_id) = 0;

    // Lists all known keys, including keys whose latest version is a deletion.
    virtual std::vector<ItemKey> list_keys() const = 0;

    // Retained messages in ascending order by (last_update, last_update_server_id, uuid).
    virtual std::vector<entity::EntityMessage> history(std::string_view application_name,
                                                       std::string_view key) const = 0;
    virtual std::expected<entity::EntityMessage, MessageUnavailable> find_message(const Uuid& uuid) const = 0;
    virtual std::vector<entity::EntityMessage> pending_for_node(std::int32_t node_id) const = 0;

    // Reclaims payloads and settled acknowledgements while preserving UUIDs for deduplication.
    // keep_latest must be positive. Returns the number of messages reclaimed for this key.
    virtual std::size_t recycle(std::string_view application_name, std::string_view key,
                                std::size_t keep_latest) = 0;
  };
}
