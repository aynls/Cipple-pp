#pragma once
#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

#include <boost/asio/ip/tcp.hpp>

namespace cipple::sharding
{
  struct ShardMember
  {
    std::int32_t id;
    boost::asio::ip::tcp::endpoint application;
    boost::asio::ip::tcp::endpoint raft;
  };

  struct Shard
  {
    std::uint32_t id;
    std::vector<ShardMember> members;
  };

  class ShardLayout
  {
  public:
    static ShardLayout load(const std::filesystem::path& path);

    const Shard& route(std::string_view application, std::string_view key) const noexcept;
    const Shard* find(std::uint32_t id) const noexcept;
    const std::string& identity() const noexcept;

  private:
    ShardLayout(std::vector<Shard> shards, std::string identity);

    std::vector<Shard> _shards;
    std::string _identity;
  };
}
