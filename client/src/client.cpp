#include "client.hpp"

#include <boost/asio/error.hpp>

#include <utility>

#include "message/delete_request.hpp"
#include "message/delete_response.hpp"
#include "message/dispatch_request.hpp"
#include "message/dispatch_response.hpp"
#include "message/get_request.hpp"
#include "message/get_response.hpp"
#include "message/put_request.hpp"
#include "message/put_response.hpp"
#include "message/subscribe_request.hpp"
#include "message/subscribe_response.hpp"
#include "message/unsubscribe_request.hpp"
#include "message/unsubscribe_response.hpp"
#include "tcp/connector.hpp"

namespace cipple::client
{
  namespace
  {
    namespace asio = boost::asio;
    using boost::system::error_code;
    using message::MessageType;

    template<typename Response>
    auto status_handler(Client::StatusHandler on_done)
    {
      return [on_done = std::move(on_done)](const error_code error, const message::Message* response) {
        if (on_done)
        {
          on_done(error, !error && static_cast<const Response&>(*response).success());
        }
      };
    }
  }

  std::shared_ptr<Client> Client::start(asio::io_context& context, const asio::ip::tcp::endpoint& endpoint,
                                        ClientHandlers handlers)
  {
    auto client = std::shared_ptr<Client>{new Client{std::move(handlers)}};
    const auto weak = client->weak_from_this();
    client->_connector = tcp::Connector::start(context, endpoint, {
      .on_connected = [weak](std::shared_ptr<tcp::Session> session) {
        if (const auto self = weak.lock())
        {
          self->_session = std::move(session);
          if (self->_handlers.on_connected)
          {
            self->_handlers.on_connected();
          }
        }
      },
      .on_message = [weak](tcp::Session& session, std::unique_ptr<message::Message> message) {
        if (const auto self = weak.lock())
        {
          self->on_message(session, *message);
        }
      },
      .on_disconnected = [weak](const error_code reason) {
        if (const auto self = weak.lock())
        {
          self->_session.reset();
          self->fail_pending(reason);
          if (self->_connector && self->_handlers.on_disconnected)
          {
            self->_handlers.on_disconnected(reason);
          }
        }
      },
    });
    return client;
  }

  Client::Client(ClientHandlers handlers) : _handlers{std::move(handlers)}
  {
  }

  Client::~Client() noexcept = default;

  bool Client::is_connected() const noexcept
  {
    return _session && _session->is_open();
  }

  void Client::stop()
  {
    const auto self = shared_from_this();
    if (auto connector = std::move(_connector))
    {
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
    send(message::DeleteRequest{uuid, std::move(application_name), std::move(key)},
         MessageType::kDeleteResponse, status_handler<message::DeleteResponse>(std::move(on_done)));
  }

  void Client::get(std::string application_name, std::string key, GetHandler on_done)
  {
    send(message::GetRequest{Uuid::generate(), std::move(application_name), std::move(key)}, MessageType::kGetResponse,
      [on_done = std::move(on_done)](const error_code error, const message::Message* response) {
        if (!on_done)
        {
          return;
        }
        std::optional<std::string> value;
        if (!error)
        {
          const auto& reply = static_cast<const message::GetResponse&>(*response);
          if (!reply.items().empty() && reply.items().back().operation_type() != entity::OperationType::kDelete)
          {
            value = reply.value();
          }
        }
        on_done(error, std::move(value));
      });
  }

  void Client::subscribe(std::string application_name, std::string key, StatusHandler on_done)
  {
    send(message::SubscribeRequest{Uuid::generate(), std::move(application_name), std::move(key), "", 0},
         MessageType::kSubscribeResponse, status_handler<message::SubscribeResponse>(std::move(on_done)));
  }

  void Client::unsubscribe(std::string application_name, std::string key, StatusHandler on_done)
  {
    send(message::UnsubscribeRequest{Uuid::generate(), std::move(application_name), std::move(key), "", 0},
         MessageType::kUnsubscribeResponse, status_handler<message::UnsubscribeResponse>(std::move(on_done)));
  }

  void Client::send(const message::Message& request, const MessageType response_type, ResponseHandler on_done)
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
    if (const auto error = _session->async_send(request))
    {
      on_done(error, nullptr);
      return;
    }
    _pending.emplace(request.uuid(), PendingResponse{response_type, std::move(on_done)});
  }

  void Client::on_message(tcp::Session& session, const message::Message& message)
  {
    if (message.type() == MessageType::kDispatchRequest)
    {
      const auto& dispatch = static_cast<const message::DispatchRequest&>(message);
      const auto handled = static_cast<bool>(_handlers.on_update);
      if (handled)
      {
        _handlers.on_update(dispatch.entity());
      }
      if (session.is_open() && session.async_send(message::DispatchResponse{message.uuid(), handled}))
      {
        session.close();
      }
      return;
    }

    if (auto pending = _pending.extract(message.uuid()); !pending.empty())
    {
      auto& expected = pending.mapped();
      if (message.type() == expected.type)
      {
        expected.on_done({}, &message);
      }
      else
      {
        expected.on_done(make_error_code(boost::system::errc::protocol_error), nullptr);
      }
    }
  }

  void Client::fail_pending(const error_code reason)
  {
    auto pending = std::move(_pending);
    _pending.clear();
    for (const auto& [uuid, request]: pending)
    {
      request.on_done(reason, nullptr);
    }
  }
}
