#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>

#include <charconv>
#include <csignal>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <string_view>

#include "server.hpp"
#include "storage/sqlite_storage.hpp"

int main(const int argc, char** argv)
{
  namespace asio = boost::asio;
  if (argc != 3)
  {
    std::cerr << "Usage: cipple_server <port> <database>\n";
    return 2;
  }

  const std::string_view port_arg{argv[1]};
  const auto* end = port_arg.data() + port_arg.size();
  std::uint16_t port = 0;
  const auto parsed = std::from_chars(port_arg.data(), end, port);
  if (parsed.ec != std::errc{} || parsed.ptr != end)
  {
    std::cerr << "Port must be an integer from 0 to 65535\n";
    return 2;
  }

  try
  {
    asio::io_context context;
    const auto server = cipple::server::Server::start(context, {asio::ip::address_v4::loopback(), port},
      std::make_unique<cipple::storage::SqliteStorage>(argv[2]));
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
