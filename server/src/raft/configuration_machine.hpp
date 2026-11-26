#pragma once
#include <libnuraft/snapshot.hxx>
#include <libnuraft/state_machine.hxx>

#include <array>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

namespace cipple::storage
{
  class SqliteStorage;
}

namespace cipple::raft
{
  struct ConfigurationMachineSnapshot;

  // Persistent state machine for committed application messages. Construct this
  // before RaftNode: construction synchronously reconciles any complete inbound
  // snapshot whose final chunk was already acknowledged but whose installation
  // was interrupted by a crash. If that pending image cannot be restored the
  // constructor throws and the node must remain closed.
  class ConfigurationMachine final : public nuraft::state_machine
  {
  public:
    ConfigurationMachine(const std::filesystem::path& database, std::int32_t node_id,
                         bool clustered, std::function<void()> on_applied);
    ~ConfigurationMachine() noexcept override;

    storage::SqliteStorage& reader() const noexcept;

    nuraft::ptr<nuraft::buffer> commit(nuraft::ulong log_idx, nuraft::buffer& data) override;
    nuraft::ptr<nuraft::buffer> pre_commit(nuraft::ulong log_idx, nuraft::buffer& data) override;
    void rollback(nuraft::ulong log_idx, nuraft::buffer& data) override;
    void commit_config(nuraft::ulong log_idx, nuraft::ptr<nuraft::cluster_config>& new_conf) override;

    void save_logical_snp_obj(nuraft::snapshot& s, nuraft::ulong& obj_id,
                              nuraft::buffer& data, bool is_first_obj, bool is_last_obj) override;
    bool apply_snapshot(nuraft::snapshot& s) override;
    int read_logical_snp_obj(nuraft::snapshot& s, void*& user_snp_ctx, nuraft::ulong obj_id,
                             nuraft::ptr<nuraft::buffer>& data_out, bool& is_last_obj) override;
    void free_user_snp_ctx(void*& user_snp_ctx) override;
    nuraft::ptr<nuraft::snapshot> last_snapshot() override;
    nuraft::ulong last_commit_index() override;
    void create_snapshot(nuraft::snapshot& s,
                         nuraft::async_result<bool>::handler_type& when_done) override;
    bool chk_create_snapshot() override;
    bool allow_leadership_transfer() override;

  private:
    struct IncomingSnapshot;

    void recover_pending_snapshot();
    void install_recovered_snapshot(const ConfigurationMachineSnapshot& snapshot);
    void load_durable_snapshot();
    void cleanup_snapshot_files() const noexcept;
    void discard_incoming();
    void notify_applied() noexcept;
    void fail() noexcept;
    void ensure_open() const;

    const std::filesystem::path _database;
    const std::int32_t _node_id;
    std::function<void()> _on_applied;
    std::unique_ptr<storage::SqliteStorage> _writer;
    std::unique_ptr<storage::SqliteStorage> _reader;

    std::mutex _apply_mutex;
    mutable std::mutex _snapshot_mutex;
    std::map<std::uint64_t, ConfigurationMachineSnapshot> _snapshots;
    std::unique_ptr<IncomingSnapshot> _incoming;
    std::atomic<std::uint64_t> _last_commit_index{0};
    std::atomic_bool _failed{false};
  };
}
