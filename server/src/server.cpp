#include "server.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/system/system_error.hpp>

#include <chrono>
#include <iostream>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <vector>

#include "message/delete_request.hpp"
#include "message/delete_response.hpp"
#include "message/dispatch_request.hpp"
#include "message/get_request.hpp"
#include "message/get_response.hpp"
#include "message/put_request.hpp"
#include "message/put_response.hpp"
#include "message/subscribe_request.hpp"
#include "message/subscribe_response.hpp"
#include "message/unsubscribe_request.hpp"
#include "message/unsubscribe_response.hpp"
#include "storage/storage.hpp"
#include "tcp/session.hpp"

namespace cipple::server
{
  namespace
  {
    namespace asio = boost::asio;
    using entity::EntityMessage;
    using message::MessageType;

    void send(tcp::Session& session, const message::Message& message)
    {
      if (session.async_send(message))
      {
        session.close();
      }
    }

    std::int64_t next_timestamp(const std::optional<EntityMessage>& latest)
    {
      using namespace std::chrono;
      const auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
      if (!latest || now > latest->last_update())
      {
        return now;
      }
      if (latest->last_update() == std::numeric_limits<std::int64_t>::max())
      {
        throw std::overflow_error{"configuration version exhausted"};
      }
      return latest->last_update() + 1;
    }

    bool fits_read_response(const std::string_view application_name, const std::string_view key,
                            const EntityMessage::Payload& payload) noexcept
    {
      // A GET frame repeats the key and value in the resolved fields and the winning entity.
      constexpr std::size_t get_fields = 1 + 16 + 3 * 8 + 4;
      constexpr std::size_t entity_fields = 16 + (8 + 6) + 2 * 8 + 8 + 4; // Includes "update" or "delete".
      const auto* update = std::get_if<EntityMessage::Update>(&payload);
      const auto fixed_size = get_fields + entity_fields + (update ? 8 : 0);
      const auto data_size = application_name.size() + key.size() + (update ? update->value.size() : 0);
      return data_size <= (tcp::kDefaultMaxFrameLength - fixed_size) / 2;
    }

    std::string apply_increment(const std::string& base, const EntityMessage::IncrementalUpdate& update)
    {
      if (update.atomic_operation == entity::AtomicOperation::kAddEntry)
      {
        return base + update.value;
      }
      if (update.value.empty())
      {
        return base;
      }
      std::string result;
      std::size_t pos = 0;
      for (auto match = base.find(update.value); match != std::string::npos; match = base.find(update.value, pos))
      {
        result.append(base, pos, match - pos);
        pos = match + update.value.size();
      }
      result.append(base, pos, std::string::npos);
      return result;
    }
  }

  std::shared_ptr<Server> Server::start(asio::io_context& context, const asio::ip::tcp::endpoint& endpoint,
                                        std::unique_ptr<storage::Storage> storage, const std::int32_t node_id)
  {
    if (!storage)
    {
      throw std::invalid_argument{"server requires a storage backend"};
    }
    auto server = std::shared_ptr<Server>{new Server{context, endpoint, std::move(storage), node_id}};
    server->accept_next();
    return server;
  }

  Server::Server(asio::io_context& context, const asio::ip::tcp::endpoint& endpoint,
                 std::unique_ptr<storage::Storage> storage, const std::int32_t node_id)
    : _storage{std::move(storage)}
      , _acceptor{context, endpoint, false}
      , _local_endpoint{_acceptor.local_endpoint()}
      , _node_id{node_id}
  {
  }

  Server::~Server() noexcept
  {
    stop();
  }

  const asio::ip::tcp::endpoint& Server::local_endpoint() const noexcept
  {
    return _local_endpoint;
  }

  void Server::stop() noexcept
  {
    boost::system::error_code ignored;
    _acceptor.close(ignored);
    while (!_connections.empty())
    {
      auto connection = _connections.extract(_connections.begin());
      connection.mapped().session->close();
    }
  }

  void Server::accept_next()
  {
    _acceptor.async_accept([weak = weak_from_this()](const boost::system::error_code error,
                                                     asio::ip::tcp::socket socket) {
      if (const auto self = weak.lock(); self && self->_acceptor.is_open())
      {
        if (error)
        {
          self->stop();
          throw boost::system::system_error{error, "accept client"};
        }
        self->attach(std::move(socket));
        self->accept_next();
      }
    });
  }

  void Server::attach(asio::ip::tcp::socket socket)
  {
    const auto id = ++_next_connection_id;
    const auto weak = weak_from_this();
    auto session = tcp::Session::start(std::move(socket),
      [weak, id](tcp::Session& connection, std::unique_ptr<message::Message> message) {
        if (const auto self = weak.lock())
        {
          self->on_message(id, connection, *message);
        }
      },
      [weak, id](boost::system::error_code) {
        if (const auto self = weak.lock())
        {
          self->_connections.erase(id);
        }
      });
    _connections.emplace(id, Connection{std::move(session), {}});
  }

  template<typename Response, typename Request>
  void Server::write(tcp::Session& session, const Request& request, EntityMessage::Payload payload)
  {
    bool success = false;
    std::optional<EntityMessage> inserted;
    try
    {
      // A retry can also refer to a version whose payload was reclaimed.
      const auto previous = _storage->find_message(request.uuid());
      if (previous || previous.error() == storage::MessageUnavailable::kRecycled)
      {
        success = true;
      }
      else if (fits_read_response(request.application_name(), request.key(), payload))
      {
        const auto latest = _storage->latest(request.application_name(), request.key());
        EntityMessage version{request.uuid(), request.application_name(), request.key(),
                              next_timestamp(latest), _node_id, std::move(payload)};
        const auto result = _storage->append(version, {});
        success = result != storage::AppendResult::kBaseUnavailable;
        if (result == storage::AppendResult::kInserted)
        {
          inserted.emplace(std::move(version));
        }
      }
    }
    catch (const std::exception& error)
    {
      std::cerr << "Write failed: " << error.what() << '\n';
    }
    send(session, Response{request.uuid(), success});
    if (inserted)
    {
      publish(std::move(*inserted));
    }
  }

  void Server::read(tcp::Session& session, const message::GetRequest& request) const
  {
    auto latest = _storage->latest(request.application_name(), request.key());
    std::string value;
    std::vector<EntityMessage> versions;
    if (latest)
    {
      if (const auto* update = std::get_if<EntityMessage::Update>(&latest->payload()))
      {
        value = update->value;
      }
      else if (const auto* increment = std::get_if<EntityMessage::IncrementalUpdate>(&latest->payload()))
      {
        auto base = _storage->find_message(increment->base_message_uuid);
        const auto* full = base ? std::get_if<EntityMessage::Update>(&base->payload()) : nullptr;
        if (!full || base->application_name() != request.application_name() || base->key() != request.key())
        {
          throw std::runtime_error{"stored incremental update has no full base"};
        }
        value = apply_increment(full->value, *increment);
        versions.push_back(std::move(*base));
      }
      versions.push_back(std::move(*latest));
    }
    send(session, message::GetResponse{request.uuid(), request.application_name(), request.key(),
                                      std::move(value), std::move(versions)});
  }

  void Server::publish(EntityMessage version)
  {
    const ConfigKey key{version.application_name(), version.key()};
    const message::DispatchRequest event{Uuid::generate(), "", std::move(version)};
    for (auto it = _connections.begin(); it != _connections.end();)
    {
      // Closing a congested session erases its entry synchronously.
      const auto& connection = (it++)->second;
      if (connection.subscriptions.contains(key))
      {
        send(*connection.session, event);
      }
    }
  }

  void Server::on_message(const std::uint64_t connection_id, tcp::Session& session, const message::Message& message)
  {
    try
    {
      switch (message.type())
      {
      case MessageType::kPutRequest:
        {
          const auto& request = static_cast<const message::PutRequest&>(message);
          write<message::PutResponse>(session, request, EntityMessage::Update{request.value()});
          break;
        }
      case MessageType::kDeleteRequest:
        write<message::DeleteResponse>(session, static_cast<const message::DeleteRequest&>(message),
                                       EntityMessage::Delete{});
        break;
      case MessageType::kGetRequest:
        read(session, static_cast<const message::GetRequest&>(message));
        break;
      case MessageType::kSubscribeRequest:
        {
          const auto& request = static_cast<const message::SubscribeRequest&>(message);
          // Empty address and port zero select dispatch over the same TCP session.
          const bool in_session = request.callback_address().empty() && request.callback_port() == 0;
          if (in_session)
          {
            _connections.at(connection_id).subscriptions.emplace(request.application_name(), request.key());
          }
          send(session, message::SubscribeResponse{request.uuid(), in_session});
          break;
        }
      case MessageType::kUnsubscribeRequest:
        {
          const auto& request = static_cast<const message::UnsubscribeRequest&>(message);
          const bool in_session = request.callback_address().empty() && request.callback_port() == 0;
          if (in_session)
          {
            _connections.at(connection_id).subscriptions.erase({request.application_name(), request.key()});
          }
          send(session, message::UnsubscribeResponse{request.uuid(), in_session});
          break;
        }
      case MessageType::kDispatchResponse:
        break;
      default:
        session.close();
        break;
      }
    }
    catch (const std::exception& error)
    {
      std::cerr << "Request failed: " << error.what() << '\n';
      session.close();
    }
  }
}
