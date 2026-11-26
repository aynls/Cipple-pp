#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>

#include <algorithm>
#include <charconv>
#include <csignal>
#include <cstdint>
#include <exception>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "raft/raft_node.hpp"
#include "server.hpp"

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
    const auto local{address(host, port)};
    if (options.node_id <= 0 || options.snapshot_distance <= 0)
    {
      throw std::invalid_argument{"node id and snapshot distance must be positive"};
    }
    if (!options.members.empty() && port == 0)
    {
      throw std::invalid_argument{"--raft-port is required with --peers"};
    }
    options.members.push_back({options.node_id, local});

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
              << "       [--snapshot-distance <entries>]\n";
    return 2;
  }

  try
  {
    const auto port{parse_integer<std::uint16_t>(argv[1], "listen port")};
    asio::ip::address bind{asio::ip::address_v4::loopback()};
    std::optional<asio::ip::address_v4> advertised_host;
    std::uint16_t raft_port{0};
    cipple::raft::RaftOptions options;
    options.node_id = 1;
    for (int i{3}; i < argc; i += 2)
    {
      const std::string_view flag{argv[i]};
      const std::string_view value{argv[i + 1]};
      if (flag == "--bind")
      {
        bind = asio::ip::make_address(value);
      }
      else if (flag == "--id")
      {
        options.node_id = parse_integer<std::int32_t>(value, "node id");
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
    add_local_member(options, host, raft_port);

    asio::io_context context;
    const auto server{cipple::server::Server::start(context, {bind, port}, argv[2], std::move(options))};
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
