#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>

#include <charconv>
#include <csignal>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "server.hpp"
#include "storage/sqlite_storage.hpp"
#include "storage/storage_writer.hpp"

namespace
{
  namespace asio = boost::asio;

  template<typename Integer>
  Integer parse_integer(const std::string_view value, const char* what)
  {
    const auto* end = value.data() + value.size();
    Integer number{};
    const auto result = std::from_chars(value.data(), end, number);
    if (result.ec != std::errc{} || result.ptr != end)
    {
      throw std::invalid_argument{std::string{"invalid "} + what};
    }
    return number;
  }

  void parse_peers(std::string_view value, cipple::server::ServerOptions& options)
  {
    for (;;)
    {
      const auto comma = value.find(',');
      const auto peer = value.substr(0, comma);
      const auto at = peer.find('@');
      const auto colon = peer.rfind(':');
      if (at == std::string_view::npos || colon == std::string_view::npos || colon <= at + 1)
      {
        throw std::invalid_argument{"peer must use id@ip:port"};
      }
      auto ip = peer.substr(at + 1, colon - at - 1);
      if (ip.starts_with('[') && ip.ends_with(']'))
      {
        ip = ip.substr(1, ip.size() - 2);
      }
      const auto id = parse_integer<std::int32_t>(peer.substr(0, at), "peer id");
      const auto port = parse_integer<std::uint16_t>(peer.substr(colon + 1), "peer port");
      options.peers.push_back({id, {asio::ip::make_address(ip), port}});
      if (comma == std::string_view::npos)
      {
        return;
      }
      value.remove_prefix(comma + 1);
    }
  }
}

int main(const int argc, char** argv)
{
  namespace asio = boost::asio;
  if (argc < 3 || argc % 2 != 1)
  {
    std::cerr << "Usage: cipple_server <port> <database> [--bind <ip>] [--id <id>]\n"
                 "       [--peers <id@ip:port,...>] [--overlay mesh|star] [--clock-offset-ms <ms>]\n";
    return 2;
  }

  try
  {
    const auto port = parse_integer<std::uint16_t>(argv[1], "listen port");
    asio::ip::address bind = asio::ip::address_v4::loopback();
    cipple::server::ServerOptions options;
    for (int i = 3; i < argc; i += 2)
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
      else if (flag == "--peers")
      {
        parse_peers(value, options);
      }
      else if (flag == "--overlay" && (value == "mesh" || value == "star"))
      {
        options.relay_peer_updates = value == "mesh";
      }
      else if (flag == "--clock-offset-ms")
      {
        options.clock_offset_ms = parse_integer<std::int64_t>(value, "clock offset");
      }
      else
      {
        throw std::invalid_argument{std::string{"invalid server option: "} + std::string{flag}};
      }
    }
    asio::io_context context;
    // Two connections to the same file: commits run on the writer thread,
    // the I/O thread keeps its own read connection.
    auto storage = std::make_unique<cipple::storage::StorageWriter>(
      std::make_unique<cipple::storage::SqliteStorage>(argv[2]),
      std::make_unique<cipple::storage::SqliteStorage>(argv[2]));
    const auto server = cipple::server::Server::start(context, {bind, port}, std::move(storage),
      std::move(options));
    asio::signal_set signals{context, SIGINT, SIGTERM};
    signals.async_wait([&](const boost::system::error_code error, int) {
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
