#include "sharding/shard_layout.hpp"

#include <boost/asio/ip/address_v4.hpp>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <fstream>
#include <limits>
#include <map>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace cipple::sharding
{
  namespace
  {
    struct LayoutRow
    {
      std::uint32_t shard_id;
      ShardMember member;
    };

    [[noreturn]] void fail_line(const std::filesystem::path& path, const std::size_t line, const std::string& message)
    {
      throw std::invalid_argument{path.generic_string() + ':' + std::to_string(line) + ": " + message};
    }

    std::string_view trim(const std::string_view text)
    {
      std::size_t first{0};
      while (first < text.size() && std::isspace(static_cast<unsigned char>(text[first])) != 0)
      {
        ++first;
      }
      std::size_t last{text.size()};
      while (last > first && std::isspace(static_cast<unsigned char>(text[last - 1])) != 0)
      {
        --last;
      }
      return text.substr(first, last - first);
    }

    std::vector<std::string_view> split_fields(const std::string_view line)
    {
      std::vector<std::string_view> fields;
      std::size_t pos{0};
      while (pos < line.size())
      {
        while (pos < line.size() && std::isspace(static_cast<unsigned char>(line[pos])) != 0)
        {
          ++pos;
        }
        if (pos == line.size())
        {
          break;
        }
        std::size_t end{pos};
        while (end < line.size() && std::isspace(static_cast<unsigned char>(line[end])) == 0)
        {
          ++end;
        }
        fields.push_back(line.substr(pos, end - pos));
        pos = end;
      }
      return fields;
    }

    template <typename T>
    T parse_integer(const std::string_view text, const T minimum, const T maximum, const char* field,
                    const std::filesystem::path& path, const std::size_t line)
    {
      T value{};
      const auto* end{text.data() + text.size()};
      const auto result{std::from_chars(text.data(), end, value)};
      if (result.ec != std::errc{} || result.ptr != end || value < minimum || value > maximum)
      {
        fail_line(path, line, "invalid " + std::string{field} + " '" + std::string{text} + '\'');
      }
      return value;
    }

    bool is_unicast_ipv4(const boost::asio::ip::address_v4& address) noexcept
    {
      const auto value{address.to_uint()};
      return !address.is_unspecified() && !address.is_multicast() && (value & 0xFF000000U) != 0 &&
             (value & 0xF0000000U) != 0xF0000000U;
    }

    boost::asio::ip::tcp::endpoint parse_endpoint(const std::string_view text, const std::filesystem::path& path,
                                                  const std::size_t line)
    {
      const auto colon{text.find(':')};
      if (colon == std::string_view::npos || colon == 0 || colon + 1 == text.size() ||
          text.find(':', colon + 1) != std::string_view::npos)
      {
        fail_line(path, line, "endpoint must have the form IPv4:port: '" + std::string{text} + '\'');
      }

      const std::string host{text.substr(0, colon)};
      boost::system::error_code error;
      const auto address{boost::asio::ip::make_address_v4(host, error)};
      if (error)
      {
        fail_line(path, line, "endpoint host is not numeric IPv4: '" + std::string{text} + '\'');
      }
      if (!is_unicast_ipv4(address))
      {
        fail_line(path, line, "endpoint host is not unicast IPv4: '" + std::string{text} + '\'');
      }

      const auto port{parse_integer<std::uint32_t>(text.substr(colon + 1), 1U, 65535U, "endpoint port", path, line)};
      return {address, static_cast<std::uint16_t>(port)};
    }

    std::string canonical_endpoint(const boost::asio::ip::tcp::endpoint& endpoint)
    {
      return endpoint.address().to_string() + ':' + std::to_string(endpoint.port());
    }

    std::uint64_t endpoint_key(const boost::asio::ip::tcp::endpoint& endpoint) noexcept
    {
      return (static_cast<std::uint64_t>(endpoint.address().to_v4().to_uint()) << 16U) | endpoint.port();
    }

    std::string make_identity(const std::vector<Shard>& shards)
    {
      std::string identity{"cipple-shard-layout/v1\n"};
      identity += "route-hash=fnv1a64-u64be-length-prefixed/v1\n";
      for (const auto& shard : shards)
      {
        for (const auto& member : shard.members)
        {
          identity += std::to_string(shard.id);
          identity += ' ';
          identity += std::to_string(member.id);
          identity += ' ';
          identity += canonical_endpoint(member.application);
          identity += ' ';
          identity += canonical_endpoint(member.raft);
          identity += '\n';
        }
      }
      return identity;
    }

    void hash_byte(std::uint64_t& hash, const std::uint8_t byte) noexcept
    {
      constexpr std::uint64_t prime{0x100000001B3ULL};
      hash ^= byte;
      hash *= prime;
    }

    void hash_u64_be(std::uint64_t& hash, const std::uint64_t value) noexcept
    {
      for (int shift{56}; shift >= 0; shift -= 8)
      {
        hash_byte(hash, static_cast<std::uint8_t>(value >> shift));
      }
    }

    void hash_bytes(std::uint64_t& hash, const std::string_view value) noexcept
    {
      hash_u64_be(hash, static_cast<std::uint64_t>(value.size()));
      for (const char byte : value)
      {
        hash_byte(hash, static_cast<std::uint8_t>(byte));
      }
    }
  }

  ShardLayout::ShardLayout(std::vector<Shard> shards, std::string identity)
    : _shards{std::move(shards)}
      , _identity{std::move(identity)}
  {
  }

  ShardLayout ShardLayout::load(const std::filesystem::path& path)
  {
    std::error_code type_error;
    if (!std::filesystem::is_regular_file(path, type_error) || type_error)
    {
      throw std::runtime_error{"ShardLayout::load: cannot open " + path.generic_string()};
    }

    std::ifstream input{path};
    if (!input.is_open())
    {
      throw std::runtime_error{"ShardLayout::load: cannot open " + path.generic_string()};
    }

    std::vector<LayoutRow> rows;
    std::map<std::pair<std::uint32_t, std::int32_t>, std::size_t> member_lines;
    std::map<std::uint64_t, std::size_t> endpoint_lines;
    std::string raw_line;
    std::size_t line_number{0};
    while (std::getline(input, raw_line))
    {
      ++line_number;
      std::string_view content{raw_line};
      if (const auto comment{content.find('#')}; comment != std::string_view::npos)
      {
        content = content.substr(0, comment);
      }
      content = trim(content);
      if (content.empty())
      {
        continue;
      }

      const auto fields{split_fields(content)};
      if (fields.size() != 4)
      {
        fail_line(path, line_number, "expected four fields: shard-id member-id IPv4:app-port IPv4:raft-port");
      }

      const auto shard_id{parse_integer<std::uint32_t>(fields[0], 0U, std::numeric_limits<std::uint32_t>::max(),
                                                       "shard id", path, line_number)};
      const auto member_id{parse_integer<std::int32_t>(fields[1], 1, std::numeric_limits<std::int32_t>::max(),
                                                       "member id", path, line_number)};
      ShardMember member{member_id, parse_endpoint(fields[2], path, line_number),
                         parse_endpoint(fields[3], path, line_number)};

      if (const auto [it, inserted]{member_lines.emplace(std::make_pair(shard_id, member_id), line_number)}; !inserted)
      {
        fail_line(path, line_number,
                  "duplicate member id " + std::to_string(member_id) + " in shard " + std::to_string(shard_id) +
                      " (first defined on line " + std::to_string(it->second) + ')');
      }
      for (const auto& endpoint : {member.application, member.raft})
      {
        const auto key{endpoint_key(endpoint)};
        if (const auto [it, inserted]{endpoint_lines.emplace(key, line_number)}; !inserted)
        {
          fail_line(path, line_number,
                    "duplicate endpoint " + canonical_endpoint(endpoint) + " (first defined on line " +
                        std::to_string(it->second) + ')');
        }
      }

      rows.push_back({shard_id, member});
    }
    if (input.bad() || (input.fail() && !input.eof()))
    {
      throw std::runtime_error{"ShardLayout::load: failed while reading " + path.generic_string()};
    }
    if (rows.empty())
    {
      throw std::invalid_argument{"ShardLayout::load: no shard members in " + path.generic_string()};
    }

    std::sort(rows.begin(), rows.end(), [](const LayoutRow& left, const LayoutRow& right) {
      if (left.shard_id != right.shard_id)
      {
        return left.shard_id < right.shard_id;
      }
      return left.member.id < right.member.id;
    });

    std::vector<Shard> shards;
    for (auto& row : rows)
    {
      if (shards.empty() || shards.back().id != row.shard_id)
      {
        shards.push_back({row.shard_id, {}});
      }
      shards.back().members.push_back(row.member);
    }

    auto identity{make_identity(shards)};
    return ShardLayout{std::move(shards), std::move(identity)};
  }

  const Shard& ShardLayout::route(const std::string_view application, const std::string_view key) const noexcept
  {
    constexpr std::uint64_t offset_basis{0xCBF29CE484222325ULL};
    std::uint64_t hash{offset_basis};
    hash_bytes(hash, application);
    hash_bytes(hash, key);
    return _shards[static_cast<std::size_t>(hash % static_cast<std::uint64_t>(_shards.size()))];
  }

  const Shard* ShardLayout::find(const std::uint32_t id) const noexcept
  {
    const auto it{std::lower_bound(_shards.begin(), _shards.end(), id,
                                   [](const Shard& shard, const std::uint32_t wanted) { return shard.id < wanted; })};
    if (it == _shards.end() || it->id != id)
    {
      return nullptr;
    }
    return &*it;
  }

  const std::string& ShardLayout::identity() const noexcept
  {
    return _identity;
  }
}
