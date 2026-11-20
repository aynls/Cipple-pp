#include "server.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/system/system_error.hpp>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <tuple>
#include <vector>

#include "entity/resolve.hpp"
#include "message/delete_request.hpp"
#include "message/delete_response.hpp"
#include "message/dispatch_request.hpp"
#include "message/dispatch_response.hpp"
#include "message/get_request.hpp"
#include "message/get_response.hpp"
#include "message/heartbeat_response.hpp"
#include "message/incremental_update_request.hpp"
#include "message/incremental_update_response.hpp"
#include "message/put_request.hpp"
#include "message/put_response.hpp"
#include "message/subscribe_request.hpp"
#include "message/subscribe_response.hpp"
#include "message/sync_request.hpp"
#include "message/sync_response.hpp"
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
    using Clock = std::chrono::steady_clock;
    constexpr auto kDeliveryTimeout = std::chrono::seconds{2};
    constexpr auto kDeliveryCheckInterval = std::chrono::milliseconds{250};

    bool send(tcp::Session& session, const message::Message& message)
    {
      if (session.async_send(message))
      {
        session.close();
        return false;
      }
      return true;
    }

    auto version_order(const EntityMessage& message)
    {
      return std::tuple{message.last_update(), message.last_update_server_id(), message.uuid()};
    }

    std::size_t entity_size(const EntityMessage& message) noexcept
    {
      auto size = 16 + 3 * 8 + 8 + 4 + entity::operation_type_wire_name(message.operation_type()).size()
          + message.application_name().size() + message.key().size();
      if (const auto* full = std::get_if<EntityMessage::Update>(&message.payload()))
      {
        size += 8 + full->value.size();
      }
      if (const auto* delta = std::get_if<EntityMessage::IncrementalUpdate>(&message.payload()))
      {
        size += 16 + 2 * 8 + entity::atomic_operation_wire_name(delta->atomic_operation).size() + delta->value.size();
      }
      return size;
    }

    bool fits_read_response(const EntityMessage& message, const std::optional<EntityMessage>& base)
    {
      constexpr std::size_t get_fields = 1 + 16 + 3 * 8 + 4;
      auto size = get_fields + message.application_name().size() + message.key().size() + entity_size(message);
      if (const auto* full = std::get_if<EntityMessage::Update>(&message.payload()))
      {
        size += full->value.size();
      }
      if (const auto* delta = std::get_if<EntityMessage::IncrementalUpdate>(&message.payload()))
      {
        const auto* full = base ? std::get_if<EntityMessage::Update>(&base->payload()) : nullptr;
        if (!full || base->uuid() != delta->base_message_uuid || base->application_name() != message.application_name()
            || base->key() != message.key() || version_order(*base) >= version_order(message))
        {
          return false;
        }
        size += entity_size(*base) + entity::apply_increment(full->value, *delta).size();
      }
      return size <= tcp::kDefaultMaxFrameLength;
    }
  }

  std::shared_ptr<Server> Server::start(asio::io_context& context, const asio::ip::tcp::endpoint& endpoint,
                                        std::unique_ptr<storage::Storage> storage, const std::int32_t node_id)
  {
    return start(context, endpoint, std::move(storage), ServerOptions{.node_id = node_id});
  }

  std::shared_ptr<Server> Server::start(asio::io_context& context, const asio::ip::tcp::endpoint& endpoint,
                                        std::unique_ptr<storage::Storage> storage, ServerOptions options)
  {
    if (!storage)
    {
      throw std::invalid_argument{"server requires a storage backend"};
    }
    auto server = std::shared_ptr<Server>{new Server{context, endpoint, std::move(storage), options}};
    if (!options.peers.empty())
    {
      server->_replication = Replication::start(context, *server->_storage, options.node_id, std::move(options.peers));
    }
    server->accept_next();
    server->check_delivery_deadlines();
    return server;
  }

  Server::Server(asio::io_context& context, const asio::ip::tcp::endpoint& endpoint,
                 std::unique_ptr<storage::Storage> storage, const ServerOptions& options)
    : _storage{std::move(storage)}
      , _acceptor{context, endpoint, false}
      , _delivery_timer{context}
      , _local_endpoint{_acceptor.local_endpoint()}
      , _node_id{options.node_id}
      , _relay_peer_updates{options.relay_peer_updates}
      , _clock{options.clock_offset_ms}
  {
    if (!options.peers.empty() && _node_id <= 0)
    {
      throw std::invalid_argument{"cluster nodes need a positive node id"};
    }
    for (const auto& member : options.peers)
    {
      if (member.id <= 0 || member.id == _node_id || member.endpoint.port() == 0 ||
          std::ranges::find(_peer_ids, member.id) != _peer_ids.end())
      {
        throw std::invalid_argument{"invalid or duplicate peer in cluster membership"};
      }
      _peer_ids.push_back(member.id);
    }
    _clock.observe(_storage->version_watermark());
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
    try
    {
      _delivery_timer.cancel();
    }
    catch (const boost::system::system_error&)
    {
      // The timer callback also checks the closed acceptor.
    }
    if (_replication)
    {
      _replication->stop();
    }
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
      // An accepted retry must have the same operation and payload.
      const auto previous = _storage->find_message(request.uuid());
      if (previous)
      {
        success = previous->application_name() == request.application_name() && previous->key() == request.key()
               && previous->payload() == payload;
      }
      else if (previous.error() == storage::MessageUnavailable::kMissing)
      {
        EntityMessage version{request.uuid(), request.application_name(), request.key(),
                              _clock.tick(), _node_id, std::move(payload)};
        if (fits_read_response(version, find_base(version)))
        {
          success = _storage->append(version, _peer_ids) == storage::AppendResult::kInserted;
          if (success)
          {
            inserted.emplace(std::move(version));
          }
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
      if (_replication)
      {
        _replication->notify();
      }
      publish(*inserted, true);
    }
  }

  std::optional<EntityMessage> Server::find_base(const EntityMessage& message) const
  {
    if (const auto* delta = std::get_if<EntityMessage::IncrementalUpdate>(&message.payload()))
    {
      if (auto base = _storage->find_message(delta->base_message_uuid))
      {
        return std::move(*base);
      }
    }
    return std::nullopt;
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
        auto base = find_base(*latest);
        const auto* full = base ? std::get_if<EntityMessage::Update>(&base->payload()) : nullptr;
        if (!full || base->application_name() != request.application_name() || base->key() != request.key())
        {
          throw std::runtime_error{"stored incremental update has no full base"};
        }
        value = entity::apply_increment(full->value, *increment);
        versions.push_back(std::move(*base));
      }
      versions.push_back(std::move(*latest));
    }
    send(session, message::GetResponse{request.uuid(), request.application_name(), request.key(),
                                      std::move(value), std::move(versions)});
  }

  void Server::sync(tcp::Session& session, const message::SyncRequest& request)
  {
    using message::Result;
    const auto& version = request.entity();
    const auto origin = version.last_update_server_id();
    const auto configured = [this](const std::int32_t id) {
      return std::ranges::find(_peer_ids, id) != _peer_ids.end();
    };
    if (!configured(request.from_id()) || (origin != 0 && origin != _node_id && !configured(origin)))
    {
      send(session, message::SyncResponse{request.uuid(), Result::kRejected});
      return;
    }

    const auto existing = _storage->find_message(version.uuid());
    if (existing || existing.error() == storage::MessageUnavailable::kRecycled)
    {
      bool identical = existing && *existing == version;
      if (identical && request.base())
      {
        const auto stored_base = find_base(version);
        identical = stored_base == request.base();
      }
      send(session, message::SyncResponse{request.uuid(), identical ? Result::kDuplicated : Result::kRejected});
      return;
    }

    const auto& supplied = request.base();
    const auto base = supplied ? supplied : find_base(version);
    if ((supplied && version.operation_type() != entity::OperationType::kIncrementalUpdate)
        || !fits_read_response(version, base))
    {
      send(session, message::SyncResponse{request.uuid(), Result::kRejected});
      return;
    }
    auto next_clock = _clock;
    next_clock.observe(version.last_update());
    std::vector<std::int32_t> targets;
    if (_relay_peer_updates)
    {
      for (const auto peer : _peer_ids)
      {
        // The sender and original writer already hold this version.
        if (peer != request.from_id() && peer != origin)
        {
          targets.push_back(peer);
        }
      }
    }
    const auto new_base = supplied && _storage->journal_sequence(supplied->uuid()) == 0;
    const auto result = _storage->append(version, targets, supplied ? &*supplied : nullptr);
    if (result != storage::AppendResult::kInserted)
    {
      send(session, message::SyncResponse{request.uuid(), Result::kRejected});
      return;
    }
    _clock = next_clock;
    send(session, message::SyncResponse{request.uuid(), Result::kSuccess});
    if (_replication && !targets.empty())
    {
      _replication->notify();
    }
    if (new_base)
    {
      publish(*supplied, false);
    }
    const auto latest = _storage->latest(version.application_name(), version.key());
    publish(version, latest && latest->uuid() == version.uuid());
  }

  void Server::publish(const EntityMessage& version, const bool is_current)
  {
    const ConfigKey key{version.application_name(), version.key()};
    std::optional<message::DispatchRequest> durable;
    std::optional<message::DispatchRequest> live;
    for (auto it = _connections.begin(); it != _connections.end();)
    {
      // Closing a congested session erases its entry synchronously.
      auto& connection = (it++)->second;
      const auto found = connection.subscriptions.find(key);
      if (found == connection.subscriptions.end())
      {
        continue;
      }
      auto& subscription = found->second;
      if (!subscription.cursor)
      {
        if (is_current)
        {
          if (!live)
          {
            live.emplace(Uuid::generate(), "", version, 0, find_base(version));
          }
          send(*connection.session, *live);
        }
      }
      else if (subscription.pending || subscription.needs_replay)
      {
        subscription.needs_replay = true;
      }
      else
      {
        if (!durable)
        {
          durable.emplace(Uuid::generate(), "", version, _storage->journal_sequence(version.uuid()), find_base(version));
        }
        if (durable->sequence() > *subscription.cursor)
        {
          subscription.pending = PendingDelivery{durable->uuid(), durable->sequence(), Clock::now() + kDeliveryTimeout};
          send(*connection.session, *durable);
        }
      }
    }
  }

  void Server::replay(tcp::Session& session, const ConfigKey& key, Subscription& subscription)
  {
    auto events = _storage->journal_after(key.first, key.second, *subscription.cursor, 1);
    subscription.needs_replay = !events.empty();
    if (events.empty())
    {
      return;
    }
    auto& event = events.front();
    auto base = find_base(event.message);
    const auto reply_id = Uuid::generate();
    subscription.pending = PendingDelivery{reply_id, event.sequence, Clock::now() + kDeliveryTimeout};
    send(session, message::DispatchRequest{reply_id, "", std::move(event.message), event.sequence, std::move(base)});
  }

  void Server::check_delivery_deadlines()
  {
    _delivery_timer.expires_after(kDeliveryCheckInterval);
    _delivery_timer.async_wait([weak = weak_from_this()](const boost::system::error_code error) {
      if (const auto self = weak.lock(); self && !error && self->_acceptor.is_open())
      {
        const auto now = Clock::now();
        for (auto it = self->_connections.begin(); it != self->_connections.end();)
        {
          const auto& connection = (it++)->second;
          if (std::ranges::any_of(connection.subscriptions, [&](const auto& entry) {
            const auto& pending = entry.second.pending;
            return pending && pending->deadline <= now;
          }))
          {
            connection.session->close();
          }
        }
        self->check_delivery_deadlines();
      }
    });
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
      case MessageType::kIncrementalUpdateRequest:
        {
          const auto& request = static_cast<const message::IncrementalUpdateRequest&>(message);
          write<message::IncrementalUpdateResponse>(session, request, EntityMessage::IncrementalUpdate{
              request.base_message_uuid(), request.atomic_operation(), request.value()});
          break;
        }
      case MessageType::kGetRequest:
        read(session, static_cast<const message::GetRequest&>(message));
        break;
      case MessageType::kSubscribeRequest:
        {
          const auto& request = static_cast<const message::SubscribeRequest&>(message);
          // Empty address and port zero select dispatch over the same TCP session.
          const bool in_session = request.callback_address().empty() && request.callback_port() == 0;
          const auto cursor = request.resume_after();
          const ConfigKey key{request.application_name(), request.key()};
          if (in_session)
          {
            _connections.at(connection_id).subscriptions.insert_or_assign(key, Subscription{.cursor = cursor});
          }
          if (send(session, message::SubscribeResponse{request.uuid(), in_session}) && in_session && cursor)
          {
            replay(session, key, _connections.at(connection_id).subscriptions.at(key));
          }
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
        {
          auto& subscriptions = _connections.at(connection_id).subscriptions;
          const auto it = std::ranges::find_if(subscriptions, [&](const auto& entry) {
            return entry.second.pending && entry.second.pending->reply_id == message.uuid();
          });
          if (it != subscriptions.end())
          {
            if (!static_cast<const message::DispatchResponse&>(message).success())
            {
              session.close();
              break;
            }
            auto& subscription = it->second;
            subscription.cursor = subscription.pending->sequence;
            subscription.pending.reset();
            if (subscription.needs_replay)
            {
              replay(session, it->first, subscription);
            }
          }
          break;
        }
      case MessageType::kSyncRequest:
        sync(session, static_cast<const message::SyncRequest&>(message));
        break;
      case MessageType::kHeartbeatRequest:
        send(session, message::HeartbeatResponse{message.uuid(), true});
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
