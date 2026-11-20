#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>

#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "client.hpp"
#include "entity/entity_message.hpp"

int main(const int argc, char** argv)
{
  namespace asio = boost::asio;
  using boost::system::error_code;
  using cipple::client::Client;
  using cipple::entity::EntityMessage;

  const std::string_view command = argc > 2 ? argv[2] : "";
  const bool watching = command == "subscribe";
  const bool single_key_command = command == "get" || command == "delete" || watching;
  if (!((command == "put" && argc == 6) || (single_key_command && argc == 5)))
  {
    std::cerr << "Usage: cipple_client <port> put <application> <key> <value>\n"
                 "       cipple_client <port> get|delete|subscribe <application> <key>\n";
    return 2;
  }

  const std::string_view port_arg{argv[1]};
  const auto* end = port_arg.data() + port_arg.size();
  std::uint16_t port = 0;
  const auto parsed = std::from_chars(port_arg.data(), end, port);
  if (parsed.ec != std::errc{} || parsed.ptr != end || port == 0)
  {
    std::cerr << "Port must be an integer from 1 to 65535\n";
    return 2;
  }

  try
  {
    asio::io_context context;
    asio::steady_timer timeout{context, std::chrono::seconds{10}};
    asio::signal_set signals{context, SIGINT, SIGTERM};
    int result = 1;
    const auto finish = [&](const int status) {
      result = status;
      context.stop();
    };
    timeout.async_wait([&](const error_code error) {
      if (!error)
      {
        std::cerr << "Request timed out\n";
        finish(1);
      }
    });
    signals.async_wait([&](const error_code error, const int signal) {
      if (!error)
      {
        finish(128 + signal);
      }
    });

    const auto on_status = [&](const error_code error, const bool success) {
      if (error)
      {
        std::cerr << error.message() << '\n';
        if (!watching)
        {
          finish(1);
        }
      }
      else if (!success)
      {
        std::cerr << "Request rejected\n";
        finish(1);
      }
      else if (watching)
      {
        timeout.cancel();
        std::cout << "Subscribed" << std::endl;
      }
      else
      {
        std::cout << "OK\n";
        finish(0);
      }
    };
    const auto on_get = [&](const error_code error, const std::optional<std::string> value) {
      if (error)
      {
        std::cerr << error.message() << '\n';
        finish(1);
      }
      else if (!value)
      {
        std::cerr << "Configuration not found\n";
        finish(1);
      }
      else
      {
        std::cout << *value << '\n';
        finish(0);
      }
    };

    std::shared_ptr<Client> client;
    client = Client::start(context, {asio::ip::address_v4::loopback(), port}, {
      .on_connected = [&] {
        if (command == "put")
        {
          client->put(argv[3], argv[4], argv[5], on_status);
        }
        else if (command == "delete")
        {
          client->erase(argv[3], argv[4], on_status);
        }
        else if (watching)
        {
          client->subscribe(argv[3], argv[4], on_status);
        }
        else
        {
          client->get(argv[3], argv[4], on_get);
        }
      },
      .on_disconnected = [&](const error_code error) {
        if (watching)
        {
          std::cerr << "Disconnected: " << error.message() << '\n';
        }
      },
      .on_update = [](const EntityMessage& version) {
        if (const auto* update = std::get_if<EntityMessage::Update>(&version.payload()))
        {
          std::cout << update->value << std::endl;
        }
        else if (version.operation_type() == cipple::entity::OperationType::kDelete)
        {
          std::cout << "<deleted>" << std::endl;
        }
      },
    });
    context.run();
    return result;
  }
  catch (const std::exception& error)
  {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
