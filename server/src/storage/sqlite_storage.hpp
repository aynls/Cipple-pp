#pragma once
#include <array>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <vector>

#include "storage/storage.hpp"

struct sqlite3;

namespace cipple::storage
{
  class SqliteStorage final : public Storage
  {
  public:
    struct OpenOptions
    {
      bool raft{false};
      bool clustered{false};
    };

    struct RaftMutationResult
    {
      bool success{false};
      bool data_changed{false};
    };

    struct RaftGetResult
    {
      std::string value;
      std::vector<entity::EntityMessage> versions;
    };

    struct RaftSnapshotInfo
    {
      std::uint64_t log_index{0};
      std::uint64_t log_term{0};
      std::uint64_t file_size{0};
      std::array<std::uint8_t, 32> sha256{};
      // Serialized nuraft::snapshot, including its log index, term and cluster config.
      std::vector<std::uint8_t> metadata;
    };

    static constexpr std::size_t kRaftRetainedEvents{128};

    // Opens a file-backed database and requires WAL mode.
    explicit SqliteStorage(const std::filesystem::path& path);
    // Raft mode adds its bookkeeping schema without changing the legacy tables.
    SqliteStorage(const std::filesystem::path& path, const OpenOptions& options);
    ~SqliteStorage() noexcept override;

    AppendResult append(const entity::EntityMessage& message,
                        std::span<const std::int32_t> target_node_ids,
                        const entity::EntityMessage* supplied_base = nullptr) override;
    AppendOutcome append_with_outcome(const entity::EntityMessage& message,
                                      std::span<const std::int32_t> target_node_ids,
                                      const entity::EntityMessage* supplied_base = nullptr) override;
    std::vector<ItemKey> list_keys() const override;
    std::vector<entity::EntityMessage> history(std::string_view application_name,
                                               std::string_view key) const override;
    std::optional<entity::EntityMessage> latest(std::string_view application_name,
                                                std::string_view key) const override;
    std::expected<entity::EntityMessage, MessageUnavailable> find_message(const Uuid& uuid) const override;
    std::vector<PendingId> pending_ids_after(std::int32_t node_id, const std::optional<PendingCursor>& after,
                                             std::int64_t limit) const override;
    std::size_t mark_acked_batch(std::int32_t node_id, std::span<const Uuid> message_uuids) override;
    std::vector<JournalEntry> journal_after(std::string_view application_name, std::string_view key,
                                            std::int64_t after, std::int64_t limit) const override;
    std::int64_t journal_sequence(const Uuid& uuid) const override;
    std::int64_t version_watermark() const override;
    std::size_t recycle(std::string_view application_name, std::string_view key,
                        std::size_t keep_latest) override;

    // Raft state-machine operations. Each apply variant persists the applied log
    // index in the same FULL synchronous transaction as its state-machine effect.
    // command_hash is the SHA-256 digest of the original encoded request.
    RaftMutationResult apply_raft_mutation(const entity::EntityMessage& message,
                                           std::span<const std::uint8_t> command_hash,
                                           std::int64_t log_index);
    RaftGetResult apply_raft_get(std::string_view application_name, std::string_view key,
                                 std::int64_t log_index);
    void apply_raft_progress(std::int64_t log_index);

    std::int64_t raft_revision_base() const noexcept;
    std::int64_t raft_applied_index() const;
    std::optional<RaftSnapshotInfo> raft_snapshot_info() const;
    void record_raft_snapshot(const RaftSnapshotInfo& snapshot);
    // Restores a complete SQLite snapshot image, then records its Raft metadata.
    // A crash between those two durable steps is repaired from the pending image
    // before the state machine is opened again.
    void restore_raft_snapshot(const std::filesystem::path& image, const RaftSnapshotInfo& snapshot);
    // Consistently copies this database using SQLite's online backup API.
    void backup_database(const std::filesystem::path& destination) const;

    // Raft replay boundaries. The floor is the newest sequence trimmed from a
    // topic; the watermark never regresses when old delivery rows are removed.
    std::int64_t journal_floor(std::string_view application_name, std::string_view key) const;
    std::int64_t journal_watermark() const;

  private:
    AppendOutcome append_impl(const entity::EntityMessage& message,
                              std::span<const std::int32_t> target_node_ids,
                              const entity::EntityMessage* supplied_base, bool check_current);

    struct CloseDatabase
    {
      void operator()(sqlite3* db) const noexcept;
    };

    std::unique_ptr<sqlite3, CloseDatabase> _db;
    mutable std::mutex _mutex;
    bool _raft_mode{false};
    std::int64_t _raft_revision_base{0};
  };
}
