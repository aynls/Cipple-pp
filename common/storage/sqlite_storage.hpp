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
    std::vector<entity::EntityMessage> pending_for_node(std::int32_t node_id) const override;
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
