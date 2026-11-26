#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace nuraft
{
  class state_machine;
}

namespace cipple::raft
{
  using Completion = std::function<void(std::optional<std::vector<std::uint8_t>>)>;

  struct RaftMember
  {
    std::int32_t id;
    std::string endpoint;
  };

  struct RaftOptions
  {
    std::int32_t node_id;
    std::vector<RaftMember> members;
    std::filesystem::path database;
    std::int32_t snapshot_distance{256};
  };

  // Owns one NuRaft server and the single worker that submits client commands
  // to it. submit() never runs replication or durable log I/O on the caller's
  // thread. An accepted completion is invoked exactly once before stop()
  // returns; std::nullopt means rejected, not committed, timed out, uncertain,
  // or stopped. Calling stop() from inside a completion is not supported.
  class RaftNode final
  {
  public:
    RaftNode(const RaftOptions& options, std::shared_ptr<nuraft::state_machine> state_machine);
    ~RaftNode() noexcept;

    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    bool submit(std::vector<std::uint8_t> command, Completion completion);
    std::int32_t leader_id() const noexcept;
    void stop() noexcept;

  private:
    class Impl;

    std::unique_ptr<Impl> _impl;
  };
}
