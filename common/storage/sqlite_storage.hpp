#pragma once
#include <filesystem>
#include <memory>
#include <mutex>

#include "storage/storage.hpp"

struct sqlite3;

namespace cipple::storage
{
  class SqliteStorage final : public Storage
  {
  public:
    // Opens a file-backed database and requires WAL mode.
    explicit SqliteStorage(const std::filesystem::path& path);
    ~SqliteStorage() noexcept override;

    AppendResult append(const entity::EntityMessage& message,
                        std::span<const std::int32_t> target_node_ids,
                        const entity::EntityMessage* supplied_base = nullptr) override;
    bool mark_acked(const Uuid& message_uuid, std::int32_t node_id) override;
    std::vector<ItemKey> list_keys() const override;
    std::vector<entity::EntityMessage> history(std::string_view application_name,
                                               std::string_view key) const override;
    std::optional<entity::EntityMessage> latest(std::string_view application_name,
                                                std::string_view key) const override;
    std::expected<entity::EntityMessage, MessageUnavailable> find_message(const Uuid& uuid) const override;
    std::vector<entity::EntityMessage> pending_for_node(std::int32_t node_id, std::int64_t limit = 0) const override;
    std::vector<Uuid> pending_ids_for_node(std::int32_t node_id, std::int64_t limit) const override;
    std::vector<PendingId> pending_ids_after(std::int32_t node_id, const std::optional<PendingCursor>& after,
                                            std::int64_t limit) const override;
    std::size_t mark_acked_batch(std::int32_t node_id, std::span<const Uuid> message_uuids) override;
    std::vector<Uuid> acknowledged_among(std::int32_t node_id, std::span<const Uuid> message_uuids) const override;
    std::vector<JournalEntry> journal_after(std::string_view application_name, std::string_view key,
                                           std::int64_t after, std::int64_t limit) const override;
    std::int64_t journal_sequence(const Uuid& uuid) const override;
    std::int64_t version_watermark() const override;
    std::size_t recycle(std::string_view application_name, std::string_view key,
                        std::size_t keep_latest) override;

  private:
    struct CloseDatabase
    {
      void operator()(sqlite3* db) const noexcept;
    };

    std::unique_ptr<sqlite3, CloseDatabase> _db;
    mutable std::mutex _mutex;
  };
}
