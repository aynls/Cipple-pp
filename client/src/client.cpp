#include "client.hpp"

#include <boost/asio/error.hpp>

#include <algorithm>
#include <stdexcept>
#include <utility>
#include <vector>

#include "protocol/cluster/history_lost_response.hpp"
#include "protocol/configuration/delete_request.hpp"
#include "protocol/configuration/delete_response.hpp"
#include "protocol/subscription/dispatch_request.hpp"
#include "protocol/subscription/dispatch_response.hpp"
#include "protocol/configuration/get_request.hpp"
#include "protocol/configuration/get_response.hpp"
#include "protocol/heartbeat/heartbeat_request.hpp"
#include "protocol/heartbeat/heartbeat_response.hpp"
#include "protocol/configuration/incremental_update_request.hpp"
#include "protocol/configuration/incremental_update_response.hpp"
#include "protocol/configuration/put_request.hpp"
#include "protocol/configuration/put_response.hpp"
#include "protocol/frame_codec.hpp"
#include "protocol/subscription/subscribe_request.hpp"
#include "protocol/subscription/subscribe_response.hpp"
#include "protocol/subscription/unsubscribe_request.hpp"
#include "protocol/subscription/unsubscribe_response.hpp"
#include "tcp/connector.hpp"

namespace cipple::client
{
  namespace
  {
    namespace asio = boost::asio;
    using boost::system::error_code;
    using entity::EntityMessage;
    using message::MessageType;

    bool uses_cluster_retry(const MessageType type) noexcept
    {
      return type == MessageType::kGetResponse || type == MessageType::kPutResponse
        || type == MessageType::kDeleteResponse || type == MessageType::kIncrementalUpdateResponse;
    }

    class ClientErrorCategory final : public boost::system::error_category
    {
    public:
      const char* name() const noexcept override
      {
        return "cipple-client";
      }

      std::string message(const int value) const override
      {
        if (value == static_cast<int>(ClientError::kWrongShard))
        {
          return "request routed to the wrong shard";
        }
        return "unknown client error";
      }
    };

    template <typename Response>
    auto status_handler(Client::StatusHandler on_done)
    {
      return [on_done = std::move(on_done)](const error_code error, const message::Message* response)
      {
        if (on_done)
        {
          on_done(error, !error && static_cast<const Response&>(*response).success());
        }
      };
    }
  }

  boost::system::error_code make_error_code(const ClientError error) noexcept
  {
    static const ClientErrorCategory category;
    return {static_cast<int>(error), category};
  }

  std::shared_ptr<Client> Client::start(asio::io_context& context, const asio::ip::tcp::endpoint& endpoint,
                                        ClientHandlers handlers)
  {
    return start_any(context, {endpoint}, std::move(handlers));
  }

  std::shared_ptr<Client> Client::start_any(asio::io_context& context,
                                            std::vector<asio::ip::tcp::endpoint> endpoints,
                                            ClientHandlers handlers, const ClientOptions options)
  {
    if (options.request_timeout <= std::chrono::steady_clock::duration::zero())
    {
      throw std::invalid_argument{"client: request timeout must be positive"};
    }
    if (options.heartbeat_interval && *options.heartbeat_interval <= std::chrono::steady_clock::duration::zero())
    {
      throw std::invalid_argument{"client: heartbeat interval must be positive"};
    }
    auto client{std::shared_ptr<Client>{new Client{context, std::move(handlers), options}}};
    const auto weak{client->weak_from_this()};
    client->_connector = tcp::Connector::start_any(context, std::move(endpoints), {
                                                     .on_connected = [weak](std::shared_ptr<tcp::Session> session)
                                                     {
                                                       if (const auto self{weak.lock()})
                                                       {
                                                         self->_session = std::move(session);
                                                         self->transmit_pending();
                                                         self->schedule_heartbeat();
                                                         if (self->_handlers.on_connected)
                                                         {
                                                           self->_handlers.on_connected();
                                                         }
                                                       }
                                                     },
                                                     .on_message = [weak](
                                                       tcp::Session& session, std::unique_ptr<message::Message> message)
                                                     {
                                                       if (const auto self{weak.lock()})
                                                       {
                                                         self->on_message(session, *message);
                                                       }
                                                     },
                                                     .on_disconnected = [weak](const error_code reason)
                                                     {
                                                       if (const auto self{weak.lock()})
                                                       {
                                                         self->_heartbeat_timer.reset();
                                                         self->_session.reset();
                                                         self->fail_session_pending(reason);
                                                         if (self->_connector && self->_handlers.on_disconnected)
                                                         {
                                                           self->_handlers.on_disconnected(reason);
                                                         }
                                                       }
                                                     },
                                                   }, {.connect_timeout = options.connect_timeout});
    return client;
  }

  Client::Client(asio::io_context& context, ClientHandlers handlers, const ClientOptions options)
    : _context{context}
      , _handlers{std::move(handlers)}
      , _request_timeout{options.request_timeout}
      , _heartbeat_interval{options.heartbeat_interval}
  {
  }

  Client::~Client() noexcept = default;

  bool Client::is_connected() const noexcept
  {
    return _session && _session->is_open();
  }

  std::optional<asio::ip::tcp::endpoint> Client::remote_endpoint() const noexcept
  {
    if (is_connected())
    {
      return _connector->endpoint();
    }
    return std::nullopt;
  }

  void Client::stop()
  {
    const auto self{shared_from_this()};
    if (auto connector{std::move(_connector)})
    {
      _heartbeat_timer.reset();
      _session.reset();
      connector->stop();
      fail_pending(asio::error::operation_aborted);
    }
  }

  void Client::put(std::string application_name, std::string key, std::string value, StatusHandler on_done,
                   const Uuid uuid)
  {
    send(message::PutRequest{uuid, std::move(application_name), std::move(key), std::move(value)},
         MessageType::kPutResponse, status_handler<message::PutResponse>(std::move(on_done)));
  }

  void Client::erase(std::string application_name, std::string key, StatusHandler on_done, const Uuid uuid)
  {
    send(message::DeleteRequest{uuid, std::move(application_name), std::move(key)}, MessageType::kDeleteResponse,
         status_handler<message::DeleteResponse>(std::move(on_done)));
  }

  void Client::incremental_update(std::string application_name, std::string key, const Uuid base_message_uuid,
                                  const entity::AtomicOperation operation, std::string value,
                                  StatusHandler on_done, const Uuid uuid)
  {
    send(message::IncrementalUpdateRequest{
           uuid, std::move(application_name), std::move(key),
           base_message_uuid, operation, std::move(value)
         },
         MessageType::kIncrementalUpdateResponse,
         status_handler<message::IncrementalUpdateResponse>(std::move(on_done)));
  }

  void Client::get_response(std::string application_name, std::string key,
                            std::function<void(error_code, const message::GetResponse *)> on_done)
  {
    send(message::GetRequest{Uuid::generate(), std::move(application_name), std::move(key)}, MessageType::kGetResponse,
         [on_done{std::move(on_done)}](const error_code error, const message::Message* response)
         {
           on_done(error, static_cast<const message::GetResponse*>(response));
         });
  }

  void Client::get(std::string application_name, std::string key, GetHandler on_done)
  {
    get_response(std::move(application_name), std::move(key),
                 [on_done{std::move(on_done)}](const error_code error, const message::GetResponse* reply)
                 {
                   if (!on_done)
                   {
                     return;
                   }
                   std::optional<std::string> value;
                   if (!error && !reply->items().empty() && reply->items().back().operation_type() !=
                     entity::OperationType::kDelete)
                   {
                     value = reply->value();
                   }
                   on_done(error, std::move(value));
                 });
  }

  void Client::get_base(std::string application_name, std::string key, GetBaseHandler on_done)
  {
    get_response(std::move(application_name), std::move(key),
                 [on_done{std::move(on_done)}](const error_code error, const message::GetResponse* reply)
                 {
                   if (!on_done)
                   {
                     return;
                   }
                   if (error || reply->items().empty() || reply->items().back().operation_type() ==
                     entity::OperationType::kDelete)
                   {
                     on_done(error, std::nullopt);
                     return;
                   }
                   const auto& latest{reply->items().back()};
                   if (const auto* increment{std::get_if<EntityMessage::IncrementalUpdate>(&latest.payload())})
                   {
                     const auto base{
                       std::ranges::find_if(reply->items(), [&](const EntityMessage& version)
                       {
                         return version.uuid() == increment->base_message_uuid && version.operation_type() ==
                           entity::OperationType::kUpdate
                           && version.application_name() == latest.application_name() && version.key() == latest.key();
                       })
                     };
                     if (base == reply->items().end())
                     {
                       on_done(make_error_code(boost::system::errc::protocol_error), std::nullopt);
                       return;
                     }
                     on_done({}, base->uuid());
                     return;
                   }
                   on_done({}, latest.uuid());
                 });
  }

  void Client::subscribe(std::string application_name, std::string key, StatusHandler on_done,
                         const std::optional<std::int64_t> resume_after, HistoryLostHandler on_history_lost)
  {
    send(message::SubscribeRequest{Uuid::generate(), std::move(application_name), std::move(key), resume_after},
         MessageType::kSubscribeResponse,
         [on_done{std::move(on_done)}, on_history_lost{std::move(on_history_lost)}](
           const error_code error, const message::Message* response)
         {
           if (!on_done)
           {
             return;
           }
           if (!error && response->type() == MessageType::kHistoryLostResponse)
           {
             if (on_history_lost)
             {
               on_history_lost(static_cast<const message::HistoryLostResponse&>(*response).minimum_cursor());
             }
             on_done({}, false);
             return;
           }
           on_done(error, !error && static_cast<const message::SubscribeResponse&>(*response).success());
         });
  }

  void Client::unsubscribe(std::string application_name, std::string key, StatusHandler on_done)
  {
    send(message::UnsubscribeRequest{Uuid::generate(), std::move(application_name), std::move(key)},
         MessageType::kUnsubscribeResponse, status_handler<message::UnsubscribeResponse>(std::move(on_done)));
  }

  void Client::send(const message::Message& request, const MessageType response_type, ResponseHandler on_done,
                    const std::optional<std::chrono::steady_clock::duration> timeout)
  {
    if (!is_connected())
    {
      on_done(asio::error::not_connected, nullptr);
      return;
    }
    if (_pending.contains(request.uuid()))
    {
      on_done(asio::error::already_started, nullptr);
      return;
    }
    tcp::EncodedFrame frame;
    try
    {
      frame = tcp::encode_frame_shared(request);
    }
    catch (const std::length_error&)
    {
      on_done(asio::error::message_size, nullptr);
      return;
    }
    const auto generation{++_request_generation};
    auto pending{
      _pending.emplace(request.uuid(),
                       PendingResponse{
                         response_type, std::move(on_done), generation,
                         asio::steady_timer{_context, timeout.value_or(_request_timeout)},
                         std::move(frame), uses_cluster_retry(response_type), false
                       }).first
    };
    if (const auto error{transmit(pending->second)})
    {
      auto failed{_pending.extract(pending)};
      failed.mapped().on_done(error, nullptr);
      return;
    }
    pending->second.deadline.async_wait(
      [weak{weak_from_this()}, uuid{request.uuid()}, generation](const error_code error)
      {
        if (const auto self{weak.lock()}; self && !error)
        {
          self->on_request_timeout(uuid, generation);
        }
      });
  }

  error_code Client::transmit(PendingResponse& pending) const
  {
    if (!is_connected())
    {
      return asio::error::not_connected;
    }
    const auto error{_session->async_send(pending.frame)};
    pending.in_flight = !error;
    return error;
  }

  void Client::transmit_pending()
  {
    std::vector<std::pair<std::uint64_t, Uuid>> retryable;
    for (const auto& [uuid, pending] : _pending)
    {
      if (pending.retryable && !pending.in_flight)
      {
        retryable.emplace_back(pending.generation, uuid);
      }
    }
    std::ranges::sort(retryable);

    std::vector<std::pair<Uuid, error_code>> failed;
    for (const auto& [generation, uuid] : retryable)
    {
      const auto pending{_pending.find(uuid)};
      if (pending == _pending.end() || pending->second.generation != generation)
      {
        continue;
      }
      if (const auto error{transmit(pending->second)})
      {
        failed.emplace_back(uuid, error);
      }
    }
    for (const auto& [uuid, error] : failed)
    {
      if (const auto pending{_pending.find(uuid)}; pending != _pending.end())
      {
        auto expired{_pending.extract(pending)};
        expired.mapped().on_done(error, nullptr);
      }
    }
  }

  void Client::schedule_heartbeat()
  {
    if (!_heartbeat_interval)
    {
      return;
    }
    _heartbeat_timer.emplace(_context, *_heartbeat_interval);
    _heartbeat_timer->async_wait(
      [weak{weak_from_this()}, session{std::weak_ptr<tcp::Session>{_session}}](const error_code error)
      {
        const auto self{weak.lock()};
        const auto connection{session.lock()};
        if (!error && self && connection && connection == self->_session)
        {
          self->send_heartbeat();
        }
      });
  }

  void Client::send_heartbeat()
  {
    send(message::HeartbeatRequest{Uuid::generate()}, MessageType::kHeartbeatResponse,
         [weak{weak_from_this()}, session{std::weak_ptr<tcp::Session>{_session}}](
           const error_code error, const message::Message* response)
         {
           const auto self{weak.lock()};
           const auto connection{session.lock()};
           if (!self || !connection || connection != self->_session)
           {
             return;
           }
           if (error || !static_cast<const message::HeartbeatResponse&>(*response).success())
           {
             connection->close();
             return;
           }
           self->schedule_heartbeat();
         }, _heartbeat_interval);
  }

  void Client::on_message(tcp::Session& session, const message::Message& message)
  {
    if (_session.get() != &session)
    {
      return;
    }
    if (message.type() == MessageType::kDispatchRequest)
    {
      const auto& dispatch{static_cast<const message::DispatchRequest&>(message)};
      bool handled{false};
      if (_handlers.on_delivery)
      {
        if (const auto endpoint{remote_endpoint()})
        {
          handled = _handlers.on_delivery(*endpoint, dispatch);
        }
      }
      else if (_handlers.on_update)
      {
        _handlers.on_update(dispatch.entity());
        handled = true;
      }
      if (session.is_open() && session.async_send(message::DispatchResponse{message.uuid(), handled}))
      {
        session.close();
      }
      return;
    }

    const auto pending{_pending.find(message.uuid())};
    if (pending == _pending.end())
    {
      return;
    }
    if (message.type() == MessageType::kWrongShardResponse)
    {
      auto failed{_pending.extract(pending)};
      failed.mapped().on_done(make_error_code(ClientError::kWrongShard), nullptr);
      return;
    }
    if (message.type() == MessageType::kRetryResponse)
    {
      if (!pending->second.retryable)
      {
        auto failed{_pending.extract(pending)};
        failed.mapped().on_done(make_error_code(boost::system::errc::protocol_error), nullptr);
        return;
      }
      pending->second.in_flight = false;
      session.close();
      return;
    }

    const bool expected_response{
      message.type() == pending->second.type
      || (pending->second.type == MessageType::kSubscribeResponse
        && message.type() == MessageType::kHistoryLostResponse)
    };
    auto completed{_pending.extract(pending)};
    if (expected_response)
    {
      completed.mapped().on_done({}, &message);
    }
    else
    {
      completed.mapped().on_done(make_error_code(boost::system::errc::protocol_error), nullptr);
    }
  }

  void Client::on_request_timeout(const Uuid& uuid, const std::uint64_t generation)
  {
    const auto pending{_pending.find(uuid)};
    if (pending != _pending.end() && pending->second.generation == generation)
    {
      auto expired{_pending.extract(pending)};
      expired.mapped().on_done(asio::error::timed_out, nullptr);
    }
  }

  void Client::fail_session_pending(const error_code reason)
  {
    std::vector<PendingResponse> failed;
    for (auto pending{_pending.begin()}; pending != _pending.end();)
    {
      if (pending->second.retryable)
      {
        pending->second.in_flight = false;
        ++pending;
        continue;
      }
      auto extracted{_pending.extract(pending++)};
      failed.push_back(std::move(extracted.mapped()));
    }
    for (auto& request : failed)
    {
      request.on_done(reason, nullptr);
    }
  }

  void Client::fail_pending(const error_code reason)
  {
    auto pending{std::move(_pending)};
    _pending.clear();
    for (const auto& [uuid, request] : pending)
    {
      request.on_done(reason, nullptr);
    }
  }
}
