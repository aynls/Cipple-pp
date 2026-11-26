#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>

#include <algorithm>
#include <charconv>
#include <csignal>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "raft/raft_node.hpp"
#include "server.hpp"
#include "sharding/shard_layout.hpp"

namespace
{
  namespace asio = boost::asio;
  using cipple::raft::RaftMember;

  template <typename Integer>
  Integer parse_integer(const std::string_view value, const char* what)
  {
    const auto* end{value.data() + value.size()};
    Integer number{};
    const auto result{std::from_chars(value.data(), end, number)};
    if (result.ec != std::errc{} || result.ptr != end)
    {
      throw std::invalid_argument{std::string{"invalid "} + what};
    }
    return number;
  }

  asio::ip::address_v4 raft_host(const std::string_view value)
  {
    const auto host{asio::ip::make_address_v4(value)};
    if (host.is_unspecified() || host.is_multicast() || host == asio::ip::address_v4::broadcast())
    {
      throw std::invalid_argument{"Raft members require a unicast IPv4 address"};
    }
    return host;
  }

  std::string address(const asio::ip::address_v4 host, const std::uint16_t port)
  {
    return host.to_string() + ':' + std::to_string(port);
  }

  void parse_peers(std::string_view value, std::vector<RaftMember>& members)
  {
    for (;;)
    {
      const auto comma{value.find(',')};
      const auto peer{value.substr(0, comma)};
      const auto at{peer.find('@')};
      const auto colon{peer.rfind(':')};
      if (at == std::string_view::npos || colon == std::string_view::npos || colon <= at + 1)
      {
        throw std::invalid_argument{"peer must use id@ipv4:raft-port"};
      }
      const auto id{parse_integer<std::int32_t>(peer.substr(0, at), "peer id")};
      const auto host{raft_host(peer.substr(at + 1, colon - at - 1))};
      const auto port{parse_integer<std::uint16_t>(peer.substr(colon + 1), "peer Raft port")};
      if (id <= 0 || port == 0)
      {
        throw std::invalid_argument{"peer id and port must be positive"};
      }
      members.push_back({id, address(host, port)});
      if (comma == std::string_view::npos)
      {
        return;
      }
      value.remove_prefix(comma + 1);
    }
  }

  void add_local_member(cipple::raft::RaftOptions& options, const asio::ip::address_v4 host,
                        const std::uint16_t port)
  {
    if (options.node_id <= 0)
    {
      throw std::invalid_argument{"node id must be positive"};
    }
    if (!options.members.empty() && port == 0)
    {
      throw std::invalid_argument{"--raft-port is required with --peers"};
    }
    options.members.push_back({options.node_id, address(host, port)});
    for (auto member{options.members.begin()}; member != options.members.end(); ++member)
    {
      if (std::any_of(options.members.begin(), member, [&](const auto& earlier)
      {
        return earlier.id == member->id || earlier.endpoint == member->endpoint;
      }))
      {
        throw std::invalid_argument{"Raft membership contains a duplicate id or endpoint"};
      }
    }
  }
}

int main(const int argc, char** argv)
{
  if (argc < 3 || argc % 2 != 1)
  {
    std::cerr << "Usage: cipple_server <port> <database> [--bind <ip>] [--id <id>]\n"
              << "       [--raft-port <port>] [--raft-host <ipv4>] [--peers <id@ipv4:raft-port,...>]\n"
              << "       [--snapshot-distance <entries>]\n"
              << "   or: cipple_server <port> <database> --shards <file> --shard-id <uint32> --id <id>\n"
              << "       [--bind <ip>] [--snapshot-distance <entries>]\n";
    return 2;
  }

  try
  {
    const auto port{parse_integer<std::uint16_t>(argv[1], "listen port")};
    asio::ip::address bind{asio::ip::address_v4::loopback()};
    std::optional<asio::ip::address> explicit_bind;
    std::optional<asio::ip::address_v4> advertised_host;
    std::optional<std::filesystem::path> shards_file;
    std::optional<std::uint32_t> shard_id;
    std::optional<std::uint16_t> raft_port;
    bool id_present{false};
    cipple::raft::RaftOptions options;
    options.node_id = 1;
    for (int i{3}; i < argc; i += 2)
    {
      const std::string_view flag{argv[i]};
      const std::string_view value{argv[i + 1]};
      if (flag == "--bind")
      {
        bind = asio::ip::make_address(value);
        explicit_bind = bind;
      }
      else if (flag == "--id")
      {
        id_present = true;
        options.node_id = parse_integer<std::int32_t>(value, "node id");
      }
      else if (flag == "--shards")
      {
        shards_file = std::filesystem::path{std::string{value}};
      }
      else if (flag == "--shard-id")
      {
        shard_id = parse_integer<std::uint32_t>(value, "shard id");
      }
      else if (flag == "--raft-port")
      {
        raft_port = parse_integer<std::uint16_t>(value, "Raft port");
      }
      else if (flag == "--raft-host")
      {
        advertised_host = raft_host(value);
      }
      else if (flag == "--peers")
      {
        parse_peers(value, options.members);
      }
      else if (flag == "--snapshot-distance")
      {
        options.snapshot_distance = parse_integer<std::int32_t>(value, "snapshot distance");
      }
      else
      {
        throw std::invalid_argument{std::string{"invalid server option: "} + std::string{flag}};
      }
    }

    if (options.snapshot_distance <= 0)
    {
      throw std::invalid_argument{"snapshot distance must be positive"};
    }
    if (shards_file.has_value() != shard_id.has_value())
    {
      throw std::invalid_argument{"--shards and --shard-id must be used together"};
    }
    const bool sharded{shards_file.has_value()};
    if (sharded && (!options.members.empty() || advertised_host || raft_port))
    {
      throw std::invalid_argument{"--shards mode cannot be combined with --peers, --raft-host, or --raft-port"};
    }

    std::optional<cipple::server::ShardOwnership> ownership;
    if (sharded)
    {
      if (!id_present || options.node_id <= 0)
      {
        throw std::invalid_argument{"a positive --id is required with --shards"};
      }
      std::shared_ptr<const cipple::sharding::ShardLayout> layout{
        std::make_shared<cipple::sharding::ShardLayout>(cipple::sharding::ShardLayout::load(*shards_file))
      };
      const auto* shard{layout->find(*shard_id)};
      if (shard == nullptr)
      {
        throw std::invalid_argument{"shard layout does not contain shard " + std::to_string(*shard_id)};
      }
      const auto member{std::find_if(shard->members.begin(), shard->members.end(), [&](const auto& candidate)
      {
        return candidate.id == options.node_id;
      })};
      if (member == shard->members.end())
      {
        throw std::invalid_argument{"shard " + std::to_string(*shard_id) + " has no member "
                                    + std::to_string(options.node_id)};
      }
      if (port != member->application.port())
      {
        throw std::invalid_argument{"application port does not match the shard layout"};
      }
      if (port != 0 && member->raft.port() == port)
      {
        throw std::invalid_argument{"application and Raft ports must be different"};
      }

      const auto application_address{member->application.address()};
      if (explicit_bind)
      {
        const bool wildcard{explicit_bind->is_v4() && explicit_bind->to_v4().is_unspecified()};
        if (*explicit_bind != application_address && !wildcard)
        {
          throw std::invalid_argument{"--bind must be the shard application address or IPv4 wildcard"};
        }
      }
      else
      {
        bind = application_address;
      }

      ownership = cipple::server::ShardOwnership{*shard_id, std::move(layout)};
    }
    else
    {
      if (!advertised_host && !options.members.empty() && (!bind.is_v4() || bind.is_unspecified()))
      {
        throw std::invalid_argument{"--raft-host is required when the application bind has no IPv4 address"};
      }
      const auto host{advertised_host ? *advertised_host
        : bind.is_v4() && !bind.is_unspecified() ? raft_host(bind.to_string()) : asio::ip::address_v4::loopback()};
      if (port != 0 && raft_port == port)
      {
        throw std::invalid_argument{"application and Raft ports must be different"};
      }
      add_local_member(options, host, raft_port.value_or(0));
    }

    asio::io_context context;
    const auto server{
      cipple::server::Server::start(context, {bind, port}, argv[2], std::move(options), std::move(ownership))
    };
    asio::signal_set signals{context, SIGINT, SIGTERM};
    signals.async_wait([&](const boost::system::error_code error, int)
    {
      if (!error)
      {
        server->stop();
      }
    });

    std::cout << "Listening on " << server->local_endpoint() << std::endl;
    context.run();
    return 0;
  }
  catch (const std::exception& error)
  {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
