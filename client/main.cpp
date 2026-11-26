#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "client.hpp"
#include "entity/atomic_operation.hpp"
#include "protocol/subscription/dispatch_request.hpp"
#include "sharding/shard_layout.hpp"
#include "subscription/subscription_store.hpp"

namespace
{
  namespace asio = boost::asio;
  using asio::ip::tcp;
  using boost::system::error_code;
  using cipple::Uuid;
  using cipple::client::AcceptedDelivery;
  using cipple::client::Client;
  using cipple::client::SubscriptionStore;
  using cipple::entity::AtomicOperation;

  enum class Command
  {
    kPut,
    kGet,
    kGetBase,
    kDelete,
    kIncremental,
    kSubscribe,
  };

  enum class Selector
  {
    kOrdered,
    kHash,
  };

  struct Options
  {
    std::vector<tcp::endpoint> endpoints;
    Command command{Command::kGet};
    Selector selector{Selector::kOrdered};
    std::string application;
    std::string key;
    std::string value;
    std::optional<Uuid> base;
    std::optional<AtomicOperation> operation;
    std::optional<std::string> state;
    std::optional<Uuid> request_id;
    std::optional<std::int64_t> resume_after;
    std::optional<std::uint64_t> unsubscribe_after;
    std::optional<std::chrono::milliseconds> heartbeat_interval;
    std::optional<std::filesystem::path> shard_layout;
    std::chrono::milliseconds timeout{std::chrono::seconds{10}};
    bool verbose{false};
  };

  bool writes_configuration(const Command command) noexcept
  {
    return command == Command::kPut || command == Command::kDelete || command == Command::kIncremental;
  }

  template <typename Integer>
  Integer parse_integer(const std::string_view value, const char* error)
  {
    if (!value.empty())
    {
      Integer parsed{};
      const auto* end{value.data() + value.size()};
      const auto result{std::from_chars(value.data(), end, parsed)};
      if (result.ec == std::errc{} && result.ptr == end)
      {
        return parsed;
      }
    }
    throw std::invalid_argument{error};
  }

  tcp::endpoint parse_endpoint(const std::string_view input)
  {
    const auto separator{input.rfind(':')};
    const auto port_string{separator == std::string_view::npos ? input : input.substr(separator + 1)};
    const auto port{parse_integer<std::uint16_t>(port_string, "Port must be an integer from 1 to 65535")};
    if (port == 0)
    {
      throw std::invalid_argument{"Port must be an integer from 1 to 65535"};
    }
    if (separator == std::string_view::npos)
    {
      return {asio::ip::address_v4::loopback(), port};
    }

    auto host{input.substr(0, separator)};
    if (host == "localhost")
    {
      return {asio::ip::address_v4::loopback(), port};
    }
    if (host.starts_with('[') && host.ends_with(']'))
    {
      host.remove_prefix(1);
      host.remove_suffix(1);
    }
    else if (host.find(':') != std::string_view::npos)
    {
      throw std::invalid_argument{"Enclose IPv6 addresses in brackets"};
    }
    error_code error;
    const auto address{asio::ip::make_address(host, error)};
    if (error)
    {
      throw std::invalid_argument{"Invalid server IP address"};
    }
    return {address, port};
  }

  std::vector<tcp::endpoint> parse_endpoints(std::string_view addresses)
  {
    std::vector<tcp::endpoint> endpoints;
    for (;;)
    {
      const auto comma{addresses.find(',')};
      auto endpoint{parse_endpoint(addresses.substr(0, comma))};
      if (std::ranges::find(endpoints, endpoint) == endpoints.end())
      {
        endpoints.push_back(std::move(endpoint));
      }
      if (comma == std::string_view::npos)
      {
        return endpoints;
      }
      addresses.remove_prefix(comma + 1);
    }
  }

  std::vector<tcp::endpoint> application_endpoints(const cipple::sharding::Shard& shard)
  {
    std::vector<tcp::endpoint> endpoints;
    endpoints.reserve(shard.members.size());
    for (const auto& member : shard.members)
    {
      endpoints.push_back(member.application);
    }
    return endpoints;
  }

  std::uint64_t key_hash(const std::string_view application, const std::string_view key) noexcept
  {
    constexpr std::uint64_t prime{1099511628211ULL};
    std::uint64_t hash{14695981039346656037ULL};
    for (const auto part : {application, key})
    {
      for (const auto byte : part)
      {
        hash = (hash ^ static_cast<unsigned char>(byte)) * prime;
      }
      hash *= prime; // CLI arguments cannot contain the NUL separator.
    }
    return hash;
  }

  Options parse_options(const int argc, char** argv)
  {
    Options options;
    int index{1};
    while (index < argc && std::string_view{argv[index]}.starts_with("--"))
    {
      const std::string_view flag{argv[index++]};
      if (flag == "--verbose")
      {
        options.verbose = true;
        continue;
      }
      if (index == argc)
      {
        throw std::invalid_argument{"Missing value for " + std::string{flag}};
      }
      const std::string_view value{argv[index++]};
      if (flag == "--shards")
      {
        options.shard_layout = std::filesystem::path{std::string{value}};
      }
      else if (flag == "--state")
      {
        options.state.emplace(value);
      }
      else if (flag == "--request-id")
      {
        options.request_id = Uuid::parse(value);
        if (!options.request_id)
        {
          throw std::invalid_argument{"Invalid request UUID"};
        }
      }
      else if (flag == "--timeout-ms")
      {
        const auto milliseconds{parse_integer<std::uint32_t>(value, "Invalid request timeout")};
        if (milliseconds == 0)
        {
          throw std::invalid_argument{"Request timeout must be positive"};
        }
        options.timeout = std::chrono::milliseconds{milliseconds};
      }
      else if (flag == "--heartbeat-ms")
      {
        const auto milliseconds{parse_integer<std::uint32_t>(value, "Invalid heartbeat interval")};
        if (milliseconds == 0)
        {
          throw std::invalid_argument{"Heartbeat interval must be positive"};
        }
        options.heartbeat_interval = std::chrono::milliseconds{milliseconds};
      }
      else if (flag == "--resume-after")
      {
        const auto cursor{parse_integer<std::int64_t>(value, "Invalid subscription resume cursor")};
        if (cursor < 0)
        {
          throw std::invalid_argument{"Subscription resume cursor must be non-negative"};
        }
        options.resume_after = cursor;
      }
      else if ((flag == "--selector" || flag == "--route") && (value == "ordered" || value == "hash"))
      {
        options.selector = value == "hash" ? Selector::kHash : Selector::kOrdered;
      }
      else if (flag == "--selector" || flag == "--route")
      {
        throw std::invalid_argument{"Expected ordered or hash for " + std::string{flag}};
      }
      else if (flag == "--unsubscribe-after")
      {
        options.unsubscribe_after = parse_integer<std::uint64_t>(value, "Invalid unsubscribe delivery count");
      }
      else
      {
        throw std::invalid_argument{"Unknown option " + std::string{flag}};
      }
    }
    if (options.shard_layout)
    {
      if (argc - index < 3)
      {
        throw std::invalid_argument{"Expected command, application and key after --shards"};
      }
    }
    else
    {
      if (argc - index < 4)
      {
        throw std::invalid_argument{"Expected endpoints, command, application and key"};
      }
      options.endpoints = parse_endpoints(argv[index++]);
    }
    const std::string_view command{argv[index++]};
    options.application = argv[index++];
    options.key = argv[index++];
    const int arguments{argc - index};
    if (command == "put" && arguments == 1)
    {
      options.command = Command::kPut;
      options.value = argv[index];
    }
    else if (command == "get" && arguments == 0)
    {
      options.command = Command::kGet;
    }
    else if (command == "get-base" && arguments == 0)
    {
      options.command = Command::kGetBase;
    }
    else if (command == "delete" && arguments == 0)
    {
      options.command = Command::kDelete;
    }
    else if (command == "incremental" && arguments == 3)
    {
      options.command = Command::kIncremental;
      options.base = Uuid::parse(argv[index++]);
      options.operation = cipple::entity::atomic_operation_parse(argv[index++]);
      options.value = argv[index];
      if (!options.base || !options.operation)
      {
        throw std::invalid_argument{"Expected a base UUID and add_entry or remove_entry"};
      }
    }
    else if (command == "subscribe" && arguments == 0)
    {
      options.command = Command::kSubscribe;
    }
    else
    {
      throw std::invalid_argument{"Invalid command or arguments"};
    }

    if ((options.state || options.resume_after || options.unsubscribe_after || options.heartbeat_interval)
      && options.command != Command::kSubscribe)
    {
      throw std::invalid_argument{
        "--state, --resume-after, --unsubscribe-after and --heartbeat-ms apply to subscriptions"
      };
    }
    if (options.request_id && !writes_configuration(options.command))
    {
      throw std::invalid_argument{"--request-id requires a write command"};
    }
    if (options.shard_layout)
    {
      const auto layout{cipple::sharding::ShardLayout::load(*options.shard_layout)};
      options.endpoints = application_endpoints(layout.route(options.application, options.key));
    }
    if (options.selector == Selector::kHash)
    {
      const auto selected{key_hash(options.application, options.key) % options.endpoints.size()};
      std::rotate(options.endpoints.begin(), options.endpoints.begin() +
                  static_cast<std::vector<tcp::endpoint>::difference_type>(selected), options.endpoints.end());
    }
    return options;
  }

  std::string endpoint_name(const tcp::endpoint& endpoint)
  {
    std::ostringstream name;
    name << endpoint;
    return name.str();
  }

  int run(const Options& options)
  {
    const bool watching{options.command == Command::kSubscribe};
    const bool writing{writes_configuration(options.command)};
    auto request_id{options.request_id};
    if (writing && !request_id)
    {
      request_id = Uuid::generate();
    }
    std::optional<SubscriptionStore> state;
    if (options.state)
    {
      state.emplace(*options.state);
    }

    asio::io_context context;
    asio::steady_timer deadline{context, options.timeout};
    asio::signal_set signals{context, SIGINT, SIGTERM};
    int result{1};
    std::shared_ptr<Client> client;
    std::optional<std::int64_t> resume_override{options.resume_after};
    std::uint64_t deliveries{0};
    bool command_started{false};
    bool history_lost{false};
    bool unsubscribing{false};
    bool finished{false};

    const auto finish{
      [&](const int status)
      {
        finished = true;
        result = status;
        deadline.cancel();
        context.stop();
      }
    };
    const auto report_error{
      [&](const error_code error)
      {
        if (finished)
        {
          return;
        }
        const bool wrong_shard{
          error == cipple::client::make_error_code(cipple::client::ClientError::kWrongShard)
        };
        if (writing && command_started && !wrong_shard)
        {
          std::cerr << "Write result unknown for " << request_id->to_string()
            << ". Retry with the same --request-id and "
            << (options.shard_layout ? "the same shard layout.\n" : "the same cluster endpoint list.\n");
        }
        if (error == asio::error::timed_out && options.endpoints.size() == 1 && !watching)
        {
          if (options.shard_layout)
          {
            std::cerr << "If this endpoint is a follower, check that the shard layout lists every application endpoint "
              "in the shard; redirect addresses are not trusted.\n";
          }
          else
          {
            std::cerr << "If this endpoint is a follower, provide a comma-separated list of the cluster's "
              "application endpoints; redirect addresses are not trusted.\n";
          }
        }
        std::cerr << (error == asio::error::timed_out ? "Request timed out" : error.message()) << '\n';
        finish(1);
      }
    };
    const auto on_status{
      [&](const error_code error, const bool success)
      {
        if (finished)
        {
          return;
        }
        if (error)
        {
          report_error(error);
        }
        else if (!success)
        {
          std::cerr << "Request rejected\n";
          finish(1);
        }
        else
        {
          std::cout << "OK\n";
          finish(0);
        }
      }
    };
    const auto unsubscribe{
      [&]
      {
        if (unsubscribing || finished)
        {
          return;
        }
        unsubscribing = true;
        if (!client || !client->is_connected())
        {
          finish(0);
          return;
        }
        client->unsubscribe(options.application, options.key, [&](const error_code error, const bool success)
        {
          if (error)
          {
            report_error(error);
          }
          else if (!success)
          {
            std::cerr << "Unsubscribe rejected\n";
            finish(1);
          }
          else
          {
            std::cout << "Unsubscribed" << std::endl;
            finish(0);
          }
        });
      }
    };

    deadline.async_wait([&](const error_code error)
    {
      if (!error)
      {
        report_error(asio::error::timed_out);
      }
    });
    signals.async_wait([&](const error_code error, const int signal)
    {
      if (!error)
      {
        if (watching)
        {
          unsubscribe();
        }
        else
        {
          finish(128 + signal);
        }
      }
    });

    const auto display{
      [&](const error_code error, const std::optional<std::string>& value)
      {
        if (finished)
        {
          return;
        }
        if (error)
        {
          report_error(error);
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
      }
    };
    const auto on_connected{
      [&]
      {
        if (finished)
        {
          return;
        }
        const auto endpoint{client->remote_endpoint()};
        if (options.verbose || options.endpoints.size() > 1 || state)
        {
          std::cerr << "Connected to " << *endpoint << '\n';
        }
        if (!watching)
        {
          if (command_started)
          {
            return;
          }
          command_started = true;
        }

        switch (options.command)
        {
        case Command::kPut:
          client->put(options.application, options.key, options.value, on_status, *request_id);
          break;
        case Command::kGet:
          client->get(options.application, options.key, display);
          break;
        case Command::kGetBase:
          client->get_base(options.application, options.key, [&](const error_code error, const std::optional<Uuid> base)
          {
            display(error, base ? std::make_optional(base->to_string()) : std::nullopt);
          });
          break;
        case Command::kDelete:
          client->erase(options.application, options.key, on_status, *request_id);
          break;
        case Command::kIncremental:
          client->incremental_update(options.application, options.key, *options.base, *options.operation,
                                     options.value, on_status, *request_id);
          break;
        case Command::kSubscribe:
        {
          std::optional<std::int64_t> cursor;
          if (state)
          {
            cursor = resume_override ? *resume_override
              : state->cursor(endpoint_name(*endpoint), options.application, options.key);
          }
          else if (options.resume_after)
          {
            cursor = *options.resume_after;
          }
          history_lost = false;
          client->subscribe(options.application, options.key, [&](const error_code error, const bool success)
          {
            if (error)
            {
              if (client->is_connected())
              {
                report_error(error);
              }
            }
            else if (!success)
            {
              if (!history_lost)
              {
                std::cerr << "Subscribe rejected\n";
              }
              finish(1);
            }
            else
            {
              deadline.cancel();
              std::cout << "Subscribed" << std::endl;
              if (options.unsubscribe_after && *options.unsubscribe_after == 0)
              {
                unsubscribe();
              }
            }
          }, cursor, [&](const std::int64_t minimum_cursor)
          {
            if (finished)
            {
              return;
            }
            history_lost = true;
            std::cerr << "Subscription history is unavailable at the requested cursor; minimum available cursor is "
              << minimum_cursor << ". Use --resume-after " << minimum_cursor << " to continue.\n";
          });
          break;
        }
        }
      }
    };

    auto heartbeat_interval{options.heartbeat_interval};
    if (state && !heartbeat_interval)
    {
      heartbeat_interval = std::chrono::seconds{1};
    }
    const auto endpoint_count{static_cast<std::chrono::milliseconds::rep>(options.endpoints.size())};
    const auto connect_timeout{
      std::clamp(options.timeout / endpoint_count,
                 std::chrono::milliseconds{1}, std::chrono::milliseconds{5000})
    };
    client = Client::start_any(context, options.endpoints, {
                                 .on_connected = on_connected,
                                 .on_disconnected = [&](const error_code error)
                                 {
                                   if (watching)
                                   {
                                     std::cerr << "Disconnected: " << error.message() << '\n';
                                   }
                                 },
                                 .on_delivery = [&](const tcp::endpoint& endpoint,
                                                    const cipple::message::DispatchRequest& dispatch)
                                 {
                                   if (finished)
                                   {
                                     return false;
                                   }
                                   try
                                   {
                                     const auto& version{dispatch.entity()};
                                     if (!watching || version.application_name() != options.application || version.key()
                                       != options.key)
                                     {
                                       throw std::runtime_error{"Received a version outside the current subscription"};
                                     }
                                     const auto accepted{
                                       state
                                         ? state->accept(endpoint_name(endpoint), dispatch)
                                         : AcceptedDelivery{
                                           true, true,
                                           cipple::client::materialize_delivery(dispatch)
                                         }
                                     };
                                     if (state && resume_override && dispatch.sequence() > *resume_override)
                                     {
                                       resume_override.reset();
                                     }
                                     if (accepted.first_seen)
                                     {
                                       if (accepted.current)
                                       {
                                         std::cout << (accepted.value ? *accepted.value : "<deleted>") << std::endl;
                                       }
                                       else
                                       {
                                         std::cerr << "Historical version " << version.uuid().to_string() << '\n';
                                       }
                                       ++deliveries;
                                       if (options.unsubscribe_after && deliveries == *options.unsubscribe_after)
                                       {
                                         asio::post(context, unsubscribe);
                                       }
                                     }
                                     return true;
                                   }
                                   catch (const std::exception& error)
                                   {
                                     std::cerr << "Delivery failed: " << error.what() << '\n';
                                     finish(1);
                                     return false;
                                   }
                                 },
                               }, {
                                 .request_timeout = options.timeout, .connect_timeout = connect_timeout,
                                 .heartbeat_interval = heartbeat_interval
                               });
    context.run();
    return result;
  }
}

int main(const int argc, char** argv)
{
  Options options;
  try
  {
    options = parse_options(argc, argv);
  }
  catch (const std::invalid_argument& error)
  {
    std::cerr << error.what() << '\n'
      << "Usage: cipple_client [OPTIONS] PORT|IP:PORT[,IP:PORT...] put APP KEY VALUE\n"
      "       cipple_client [OPTIONS] ENDPOINT get|get-base|delete APP KEY\n"
      "       cipple_client [OPTIONS] ENDPOINT incremental APP KEY BASE_UUID add_entry|remove_entry VALUE\n"
      "       cipple_client [--state DB] [--resume-after N] [--unsubscribe-after COUNT] ENDPOINT subscribe APP KEY\n"
      "       cipple_client [OPTIONS] --shards FILE COMMAND APP KEY [ARGS...]\n"
      "Options: --shards FILE (route by APP and KEY; omit ENDPOINT), --selector ordered|hash (alias --route)\n"
      "         --timeout-ms MS, --verbose, --request-id UUID (write command)\n"
      "         --heartbeat-ms MS (subscribe, defaults to 1000 with --state)\n"
      "Shard mode uses only the selected shard's application endpoints; wrong-shard replies are fatal.\n"
      "Ctrl+C unsubscribes on the active subscription connection.\n";
    return 2;
  }
  catch (const std::runtime_error& error)
  {
    std::cerr << error.what() << '\n';
    return 1;
  }
  try
  {
    return run(options);
  }
  catch (const std::exception& error)
  {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
