#pragma once

#include "raft/raft_node.hpp"

#include <libnuraft/log_store.hxx>
#include <libnuraft/state_mgr.hxx>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace cipple::raft
{
  class RaftDatabase;

  bool has_initialized_database(const RaftOptions& options);

  // SQLite-backed NuRaft log store. Every mutating method commits before it
  // returns; with WAL and synchronous=FULL that commit is durable, so a Raft
  // append response is never generated for a merely cached entry.
  class RaftLogStore final : public nuraft::log_store
  {
  public:
    ~RaftLogStore() noexcept override;

    RaftLogStore(const RaftLogStore&) = delete;
    RaftLogStore& operator=(const RaftLogStore&) = delete;

    nuraft::ulong next_slot() const override;
    nuraft::ulong start_index() const override;
    nuraft::ptr<nuraft::log_entry> last_entry() const override;
    nuraft::ulong append(nuraft::ptr<nuraft::log_entry>& entry) override;
    void write_at(nuraft::ulong index, nuraft::ptr<nuraft::log_entry>& entry) override;
    void end_of_append_batch(nuraft::ulong start, nuraft::ulong cnt) override;
    nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>> log_entries(nuraft::ulong start,
                                                                         nuraft::ulong end) override;
    nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>> log_entries_ext(
        nuraft::ulong start, nuraft::ulong end, nuraft::int64 batch_size_hint_in_bytes = 0) override;
    nuraft::ptr<nuraft::log_entry> entry_at(nuraft::ulong index) override;
    nuraft::ulong term_at(nuraft::ulong index) override;
    nuraft::ptr<nuraft::buffer> pack(nuraft::ulong index, nuraft::int32 cnt) override;
    void apply_pack(nuraft::ulong index, nuraft::buffer& pack) override;
    bool compact(nuraft::ulong last_log_index) override;
    bool flush() override;
    nuraft::ulong last_durable_index() override;

    // The next compaction at this index installs an authoritative snapshot.
    void expect_snapshot(nuraft::ulong index, nuraft::ulong term);

  private:
    friend class RaftStateManager;

    explicit RaftLogStore(std::shared_ptr<RaftDatabase> database);

    struct SnapshotBoundary
    {
      nuraft::ulong index;
      nuraft::ulong term;
    };

    std::shared_ptr<RaftDatabase> _database;
    std::atomic<nuraft::ulong> _start_index;
    nuraft::ulong _boundary_term{0};
    std::optional<SnapshotBoundary> _incoming_snapshot;
  };

  // State manager for a fixed-membership cluster. On first use the supplied
  // members are canonicalized by ascending node id and persisted. Later opens
  // reject a different node id or endpoint set instead of silently joining the
  // database to another cluster identity.
  class RaftStateManager final : public nuraft::state_mgr
  {
  public:
    explicit RaftStateManager(const RaftOptions& options);
    ~RaftStateManager() noexcept override;

    RaftStateManager(const RaftStateManager&) = delete;
    RaftStateManager& operator=(const RaftStateManager&) = delete;

    nuraft::ptr<nuraft::cluster_config> load_config() override;
    void save_config(const nuraft::cluster_config& config) override;
    void save_state(const nuraft::srv_state& state) override;
    nuraft::ptr<nuraft::srv_state> read_state() override;
    nuraft::ptr<nuraft::log_store> load_log_store() override;
    nuraft::int32 server_id() override;
    void system_exit(int exit_code) override;

    void check_config(const nuraft::cluster_config& config) const;

    const std::string& self_endpoint() const noexcept;
    std::uint16_t self_port() const noexcept;
    const std::vector<RaftMember>& members() const noexcept;

  private:
    std::shared_ptr<RaftDatabase> _database;
    nuraft::ptr<RaftLogStore> _log_store;
    std::int32_t _node_id;
    std::vector<RaftMember> _members;
    std::string _self_endpoint;
    std::uint16_t _self_port{0};
  };
}
