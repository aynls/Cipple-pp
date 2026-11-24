#include "server.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/system/system_error.hpp>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

#include "entity/resolve.hpp"
#include "clock/hybrid_clock.hpp"
#include "protocol/configuration/delete_request.hpp"
#include "protocol/configuration/delete_response.hpp"
#include "protocol/subscription/dispatch_response.hpp"
#include "protocol/configuration/get_request.hpp"
#include "protocol/configuration/get_response.hpp"
#include "protocol/heartbeat/heartbeat_response.hpp"
#include "protocol/configuration/incremental_update_request.hpp"
#include "protocol/configuration/incremental_update_response.hpp"
#include "protocol/configuration/put_request.hpp"
#include "protocol/configuration/put_response.hpp"
#include "protocol/subscription/subscribe_request.hpp"
#include "protocol/subscription/subscribe_response.hpp"
#include "protocol/replication/sync_request.hpp"
#include "protocol/replication/sync_response.hpp"
#include "protocol/subscription/unsubscribe_request.hpp"
#include "protocol/subscription/unsubscribe_response.hpp"
#include "storage/storage.hpp"
#include "storage/storage_writer.hpp"
#include "subscription/subscriptions.hpp"
#include "protocol/frame_codec.hpp"
#include "tcp/session.hpp"

namespace cipple::server
{
  namespace
  {
    namespace asio = boost::asio;
    using entity::EntityMessage;
    using message::MessageType;
    constexpr auto kDeliveryCheckInterval = std::chrono::milliseconds{250};

    bool send(tcp::Session& session, const message::Message& message) noexcept
    {
      try
      {
        if (!session.async_send(message))
        {
          return true;
        }
      }
      catch (const std::exception& error)
      {
        std::cerr << "Response failed: " << error.what() << '\n';
      }
      session.close();
      return false;
    }

    std::optional<EntityMessage> lookup_base(storage::Storage& store, const EntityMessage& message)
    {
      if (const auto* delta = std::get_if<EntityMessage::IncrementalUpdate>(&message.payload()))
      {
        if (auto base = store.find_message(delta->base_message_uuid))
        {
          return std::move(*base);
        }
      }
      return std::nullopt;
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

  // Writer tasks fill these objects before posting their I/O completions.
  struct Server::WriteOutcome
  {
    ConfigKey key;
    std::optional<EntityMessage> version;
    bool success = false;
    bool is_current = false;
    std::optional<std::string> error;
  };

  struct Server::SyncOutcome
  {
    ConfigKey key;
    EntityMessage version;
    std::optional<EntityMessage> base;
    message::Result result = message::Result::kRejected;
    bool is_current = false;
    std::optional<std::string> error;
  };

  std::shared_ptr<Server> Server::start(asio::io_context& context, const asio::ip::tcp::endpoint& endpoint,
                                        std::unique_ptr<storage::StorageWriter> storage, ServerOptions options)
  {
    if (!storage)
    {
      throw std::invalid_argument{"server requires a storage backend"};
    }
    auto server = std::shared_ptr<Server>{new Server{context, endpoint, std::move(storage), options}};
    if (!options.peers.empty())
    {
      server->_replication = Replication::start(context, server->_storage->reader(), *server->_storage,
                                                options.node_id, std::move(options.peers));
    }
    server->accept_next();
    server->check_delivery_deadlines();
    return server;
  }

  Server::Server(asio::io_context& context, const asio::ip::tcp::endpoint& endpoint,
                 std::unique_ptr<storage::StorageWriter> storage, const ServerOptions& options)
    : _context{context}
      , _storage{std::move(storage)}
      , _subscriptions{std::make_unique<Subscriptions>(_storage->reader())}
      , _acceptor{context, endpoint, false}
      , _delivery_timer{context}
      , _local_endpoint{_acceptor.local_endpoint()}
      , _node_id{options.node_id}
      , _relay_peer_updates{options.relay_peer_updates}
      , _clock{std::make_shared<hlc::HybridClock>(options.clock_offset_ms)}
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
    _clock->observe(_storage->reader().version_watermark());
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
    // Drain queued commits so completion posts can no longer be generated.
    _storage->stop();
    _subscriptions->stop();
  }

  void Server::accept_next()
  {
    _acceptor.async_accept([weak = weak_from_this()](const boost::system::error_code error,
                                                     asio::ip::tcp::socket socket)
    {
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
                                       [weak, id](tcp::Session& connection, std::unique_ptr<message::Message> message)
                                       {
                                         if (const auto self = weak.lock())
                                         {
                                           self->on_message(id, connection, *message);
                                         }
                                       },
                                       [weak, id](boost::system::error_code)
                                       {
                                         if (const auto self = weak.lock())
                                         {
                                           self->_subscriptions->detach(id);
                                         }
                                       });
    _subscriptions->attach(id, std::move(session));
  }

  template <typename Response, typename Request>
  void Server::write(tcp::Session& session, const Request& request, EntityMessage::Payload payload)
  {
    const auto request_id = request.uuid();
    const auto outcome = std::make_shared<WriteOutcome>(WriteOutcome{
      .key = {request.application_name(), request.key()},
    });
    const auto value_bytes = std::visit([](const auto& operation) -> std::size_t
    {
      if constexpr (requires { operation.value; })
      {
        return operation.value.size();
      }
      return 0;
    }, payload);
    const auto charge = sizeof(WriteOutcome) + 2 * (outcome->key.first.size() + outcome->key.second.size())
      + value_bytes + _peer_ids.size() * sizeof(std::int32_t);
    const bool accepted = _storage->submit(_context,
                                           [clock = _clock, node_id = _node_id, peers = _peer_ids, request_id,
                                             payload = std::move(payload), outcome](storage::Storage& store) mutable
                                           {
                                             try
                                             {
                                               // An accepted retry must have the same operation and payload.
                                               const auto previous = store.find_message(request_id);
                                               if (previous)
                                               {
                                                 outcome->success = previous->application_name() == outcome->key.first
                                                   && previous->key() == outcome->key.second && previous->payload() ==
                                                   payload;
                                                 return;
                                               }
                                               if (previous.error() != storage::MessageUnavailable::kMissing)
                                               {
                                                 return;
                                               }
                                               auto& version = outcome->version.emplace(
                                                 request_id, outcome->key.first, outcome->key.second,
                                                 clock->tick(), node_id, std::move(payload));
                                               if (fits_read_response(version, lookup_base(store, version)))
                                               {
                                                 const auto receipt = store.append_with_outcome(version, peers);
                                                 outcome->success = receipt.result == storage::AppendResult::kInserted;
                                                 outcome->is_current = receipt.is_current;
                                               }
                                             }
                                             catch (const std::exception& error)
                                             {
                                               outcome->error = error.what();
                                             }
                                           },
                                           [weak = weak_from_this(), session = session.weak_from_this(), request_id,
                                             outcome]
                                           {
                                             if (const auto self = weak.lock(); self && self->_acceptor.is_open())
                                             {
                                               const auto recipient = session.lock();
                                               self->finish_write<Response>(recipient.get(), request_id, *outcome);
                                             }
                                           }, charge);
    if (!accepted)
    {
      send(session, Response{request_id, false});
    }
  }

  template <typename Response>
  void Server::finish_write(tcp::Session* session, const Uuid& request_id, WriteOutcome& outcome)
  {
    if (outcome.error)
    {
      std::cerr << "Write failed: " << *outcome.error << '\n';
    }
    if (session && session->is_open())
    {
      send(*session, Response{request_id, outcome.success});
    }
    if (!outcome.success)
    {
      return;
    }
    if (outcome.version)
    {
      if (_replication)
      {
        _replication->notify();
      }
      if (outcome.is_current)
      {
        _subscriptions->publish_live(outcome.key, std::move(*outcome.version), std::nullopt);
      }
    }
    _subscriptions->wake(outcome.key);
    schedule_replay();
  }

  void Server::read(tcp::Session& session, const message::GetRequest& request) const
  {
    auto latest = _storage->reader().latest(request.application_name(), request.key());
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
        auto base = lookup_base(_storage->reader(), *latest);
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
    send(session, message::GetResponse{
           request.uuid(), request.application_name(), request.key(),
           std::move(value), std::move(versions)
         });
  }

  void Server::sync(tcp::Session& session, const message::SyncRequest& request)
  {
    using message::Result;
    const auto& version = request.entity();
    const auto origin = version.last_update_server_id();
    const auto configured = [this](const std::int32_t id)
    {
      return std::ranges::find(_peer_ids, id) != _peer_ids.end();
    };
    if (!configured(request.from_id()) || (origin != _node_id && !configured(origin)))
    {
      send(session, message::SyncResponse{request.uuid(), Result::kRejected});
      return;
    }

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

    const auto outcome = std::make_shared<SyncOutcome>(SyncOutcome{
      .key = {version.application_name(), version.key()},
      .version = version,
      .base = request.base(),
    });
    const auto charge = sizeof(SyncOutcome) + entity_size(version)
      + outcome->key.first.size() + outcome->key.second.size()
      + (outcome->base ? entity_size(*outcome->base) : 0) + targets.capacity() * sizeof(std::int32_t);
    const bool notify = !targets.empty();
    const bool accepted = _storage->submit(_context,
                                           [clock = _clock, targets = std::move(targets), outcome](
                                             storage::Storage& store)
                                           {
                                             try
                                             {
                                               const auto& version = outcome->version;
                                               const bool supplied = outcome->base.has_value();
                                               const auto existing = store.find_message(version.uuid());
                                               if (existing || existing.error() ==
                                                 storage::MessageUnavailable::kRecycled)
                                               {
                                                 bool identical = existing && *existing == version;
                                                 if (identical && supplied)
                                                 {
                                                   const auto stored_base = lookup_base(store, version);
                                                   identical = stored_base == outcome->base;
                                                 }
                                                 outcome->result = identical ? Result::kDuplicated : Result::kRejected;
                                                 return;
                                               }

                                               if (!fits_read_response(version, outcome->base))
                                               {
                                                 return;
                                               }
                                               auto next_clock = *clock;
                                               next_clock.observe(version.last_update());
                                               const auto receipt = store.append_with_outcome(
                                                 version, targets, supplied ? &*outcome->base : nullptr);
                                               if (receipt.result != storage::AppendResult::kInserted)
                                               {
                                                 return;
                                               }
                                               *clock = next_clock;
                                               outcome->is_current = receipt.is_current;
                                               outcome->result = Result::kSuccess;
                                             }
                                             catch (const std::exception& error)
                                             {
                                               outcome->error = error.what();
                                             }
                                           },
                                           [weak = weak_from_this(), session = session.weak_from_this(), request_id =
                                             request.uuid(), notify, outcome]
                                           {
                                             if (const auto self = weak.lock(); self && self->_acceptor.is_open())
                                             {
                                               const auto recipient = session.lock();
                                               self->finish_sync(recipient.get(), request_id, notify, *outcome);
                                             }
                                           }, charge);
    if (!accepted)
    {
      send(session, message::SyncResponse{request.uuid(), Result::kRejected});
    }
  }

  void Server::finish_sync(tcp::Session* session, const Uuid& request_id, const bool notify, SyncOutcome& outcome)
  {
    if (outcome.error)
    {
      std::cerr << "Sync failed: " << *outcome.error << '\n';
    }
    if (session && session->is_open())
    {
      send(*session, message::SyncResponse{request_id, outcome.result});
    }
    if (outcome.result == message::Result::kRejected)
    {
      return;
    }
    if (outcome.result == message::Result::kSuccess)
    {
      if (_replication && notify)
      {
        _replication->notify();
      }
      if (outcome.is_current)
      {
        _subscriptions->publish_live(outcome.key, std::move(outcome.version), std::move(outcome.base));
      }
    }
    _subscriptions->wake(outcome.key);
    schedule_replay();
  }

  void Server::schedule_replay() noexcept
  {
    if (_replay_scheduled || !_subscriptions->has_ready())
    {
      return;
    }
    try
    {
      asio::post(_context, [weak = weak_from_this()]
      {
        if (const auto self = weak.lock(); self && self->_acceptor.is_open())
        {
          self->_replay_scheduled = false;
          self->_subscriptions->replay_ready();
        }
      });
      _replay_scheduled = true;
    }
    catch (const std::exception& error)
    {
      std::cerr << "Scheduling subscription replay failed: " << error.what() << '\n';
      _subscriptions->replay_ready();
    }
  }

  void Server::check_delivery_deadlines()
  {
    _delivery_timer.expires_after(kDeliveryCheckInterval);
    _delivery_timer.async_wait([weak = weak_from_this()](const boost::system::error_code error)
    {
      if (const auto self = weak.lock(); self && !error && self->_acceptor.is_open())
      {
        self->_subscriptions->expire();
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
                                                      request.base_message_uuid(), request.atomic_operation(),
                                                      request.value()
                                                    });
          break;
        }
      case MessageType::kGetRequest:
        read(session, static_cast<const message::GetRequest&>(message));
        break;
      case MessageType::kSubscribeRequest:
        {
          const auto& request = static_cast<const message::SubscribeRequest&>(message);
          _subscriptions->subscribe(connection_id, {request.application_name(), request.key()}, request.resume_after());
          if (send(session, message::SubscribeResponse{request.uuid(), true}))
          {
            schedule_replay();
          }
          break;
        }
      case MessageType::kUnsubscribeRequest:
        {
          const auto& request = static_cast<const message::UnsubscribeRequest&>(message);
          _subscriptions->unsubscribe(connection_id, {request.application_name(), request.key()});
          send(session, message::UnsubscribeResponse{request.uuid(), true});
          break;
        }
      case MessageType::kDispatchResponse:
        {
          const auto& reply = static_cast<const message::DispatchResponse&>(message);
          _subscriptions->acknowledge(connection_id, reply.uuid(), reply.success());
          schedule_replay();
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
