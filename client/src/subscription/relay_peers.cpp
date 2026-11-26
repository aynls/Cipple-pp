#include "relay_peers.hpp"

#include <boost/asio/steady_timer.hpp>

#include <chrono>
#include <iostream>

#include "protocol/subscription/relay_messages.hpp"
#include "tcp/session.hpp"

namespace cipple::client
{
  namespace asio = boost::asio;

  namespace
  {
    constexpr auto kHandshakeTimeout{std::chrono::seconds{3}};

    bool matches(const RelayPeers::Topic& topic, const message::DispatchRequest& dispatch) noexcept
    {
      return dispatch.sequence() > 0 && topic.first == dispatch.entity().application_name()
        && topic.second == dispatch.entity().key();
    }
  }

  struct RelayPeers::Peer
  {
    std::uint64_t inbound_id{0};
    std::optional<Uuid> token;
    std::optional<asio::ip::tcp::socket> connecting;
    std::optional<asio::steady_timer> deadline;
    std::shared_ptr<tcp::Session> session;
    bool ready{false};
  };

  RelayPeers::RelayPeers(asio::io_context& context, std::weak_ptr<tcp::Session> control, DeliveryHandler deliver)
    : _context{context}, _control{std::move(control)}, _deliver{std::move(deliver)}
  {
  }

  RelayPeers::~RelayPeers() noexcept
  {
    stop();
  }

  void RelayPeers::start(const bool ipv6, const std::optional<std::uint16_t> port, const std::uint16_t slots)
  {
    std::uint16_t advertised_port{0};
    if (port)
    {
      try
      {
        const auto protocol{ipv6 ? asio::ip::tcp::v6() : asio::ip::tcp::v4()};
        _acceptor.emplace(_context, asio::ip::tcp::endpoint{protocol, *port}, false);
        advertised_port = _acceptor->local_endpoint().port();
        _slots = slots;
        accept();
      }
      catch (const boost::system::system_error& error)
      {
        std::cerr << "Relay listener unavailable: " << error.what() << '\n';
        _acceptor.reset();
        advertised_port = 0;
        _slots = 0;
      }
    }
    if (const auto control{_control.lock()})
    {
      if (control->async_send(message::RelayAdvertise{Uuid::generate(), advertised_port, _slots}))
      {
        control->close();
      }
    }
  }

  void RelayPeers::stop() noexcept
  {
    _control.reset();
    _acceptor.reset();
    while (!_incoming.empty())
    {
      const auto entry{_incoming.extract(_incoming.begin())};
      close_peer(entry.mapped());
    }
    while (!_links.empty())
    {
      revoke(_links.begin()->first);
    }
    _topics.clear();
  }

  void RelayPeers::track(Topic topic, const bool durable)
  {
    if (durable)
    {
      _topics.insert(std::move(topic));
      return;
    }
    _topics.erase(topic);
    for (auto it{_links.begin()}; it != _links.end();)
    {
      const auto& [token, link] = *it++;
      if (link.topic == topic)
      {
        revoke(token);
      }
    }
  }

  void RelayPeers::accept()
  {
    _acceptor->async_accept([weak{weak_from_this()}](const boost::system::error_code error, asio::ip::tcp::socket socket)
    {
      if (const auto self{weak.lock()}; self && self->_acceptor && !error)
      {
        if (self->_incoming.size() < self->_slots)
        {
          auto peer{std::make_shared<Peer>()};
          peer->inbound_id = ++self->_next_peer;
          self->_incoming.emplace(peer->inbound_id, peer);
          self->start_session(peer, std::move(socket));
          self->start_deadline(peer);
        }
        self->accept();
      }
    });
  }

  void RelayPeers::start_session(const std::shared_ptr<Peer>& peer, asio::ip::tcp::socket socket)
  {
    const std::weak_ptr<Peer> attempt{peer};
    const auto weak{weak_from_this()};
    peer->session = tcp::Session::start(std::move(socket),
      [weak, attempt](tcp::Session& session, std::unique_ptr<message::Message> message)
      {
        const auto self{weak.lock()};
        if (const auto peer{attempt.lock()}; self && peer)
        {
          try
          {
            self->on_peer(peer, session, *message);
          }
          catch (const std::exception& error)
          {
            std::cerr << "Relay peer message failed: " << error.what() << '\n';
            session.close();
          }
        }
      },
      [weak, attempt](boost::system::error_code)
      {
        const auto self{weak.lock()};
        if (const auto peer{attempt.lock()}; self && peer)
        {
          self->close_peer(peer);
        }
      });
  }

  void RelayPeers::start_deadline(const std::shared_ptr<Peer>& peer)
  {
    peer->deadline.emplace(_context, kHandshakeTimeout);
    peer->deadline->async_wait([weak{weak_from_this()}, attempt{std::weak_ptr<Peer>{peer}}](
      const boost::system::error_code error)
    {
      const auto self{weak.lock()};
      if (const auto peer{attempt.lock()}; self && peer && !error && !peer->ready)
      {
        self->close_peer(peer);
      }
    });
  }

  void RelayPeers::route(const message::RelayRoute& request)
  {
    const auto token{request.uuid()};
    const Topic topic{request.application_name(), request.key()};
    if (_links.contains(token))
    {
      return;
    }
    if (!_topics.contains(topic))
    {
      send_status(token, false);
      return;
    }
    if (!request.port())
    {
      std::size_t authorizations{0};
      for (const auto& [id, link] : _links)
      {
        authorizations += link.captain;
      }
      if (authorizations < _slots)
      {
        _links.emplace(token, Link{topic, true, {}});
        send_status(token, true);
      }
      else
      {
        send_status(token, false);
      }
      return;
    }

    boost::system::error_code error;
    const auto address{asio::ip::make_address(request.address(), error)};
    if (error || address.is_unspecified() || address.is_multicast())
    {
      send_status(token, false);
      return;
    }
    auto peer{std::make_shared<Peer>()};
    peer->token = token;
    peer->connecting.emplace(_context);
    _links.emplace(token, Link{topic, false, peer});
    start_deadline(peer);
    peer->connecting->async_connect({address, request.port()},
      [weak{weak_from_this()}, attempt{std::weak_ptr<Peer>{peer}}, token](const boost::system::error_code error)
      {
        const auto self{weak.lock()};
        const auto peer{attempt.lock()};
        if (!self || !peer || !peer->connecting)
        {
          return;
        }
        if (error)
        {
          self->close_peer(peer);
          return;
        }
        self->start_session(peer, std::move(*peer->connecting));
        peer->connecting.reset();
        if (peer->session->async_send(message::RelayHello{token}))
        {
          self->close_peer(peer);
        }
      });
  }

  void RelayPeers::on_peer(const std::shared_ptr<Peer>& peer, tcp::Session& session, const message::Message& message)
  {
    using message::MessageType;
    if (!peer->token)
    {
      const auto link{_links.find(message.uuid())};
      if (message.type() != MessageType::kRelayHello || link == _links.end() || !link->second.captain
        || link->second.peer)
      {
        session.close();
        return;
      }
      peer->token = link->first;
      peer->ready = true;
      link->second.peer = peer;
      _incoming.erase(peer->inbound_id);
      peer->deadline.reset();
      if (session.async_send(message::RelayStatus{*peer->token, true}))
      {
        session.close();
      }
      return;
    }
    const auto link{_links.find(*peer->token)};
    if (link == _links.end() || link->second.captain)
    {
      session.close();
      return;
    }
    if (!peer->ready && message.type() == MessageType::kRelayStatus && message.uuid() == *peer->token
      && static_cast<const message::RelayStatus&>(message).ready())
    {
      peer->ready = true;
      peer->deadline.reset();
      send_status(*peer->token, true);
      return;
    }
    if (peer->ready && message.type() == MessageType::kDispatchRequest)
    {
      const auto& dispatch{static_cast<const message::DispatchRequest&>(message)};
      if (matches(link->second.topic, dispatch))
      {
        _deliver(dispatch);
        return;
      }
    }
    session.close();
  }

  void RelayPeers::close_peer(const std::shared_ptr<Peer>& peer) noexcept
  {
    const auto token{std::exchange(peer->token, std::nullopt)};
    _incoming.erase(peer->inbound_id);
    if (token)
    {
      const auto link{_links.find(*token)};
      if (link != _links.end() && link->second.peer == peer)
      {
        _links.erase(link);
        send_status(*token, false);
      }
    }
    peer->deadline.reset();
    peer->connecting.reset();
    if (auto session{std::exchange(peer->session, {})})
    {
      session->close();
    }
  }

  void RelayPeers::revoke(const Uuid& token) noexcept
  {
    const auto link{_links.extract(token)};
    if (!link.empty() && link.mapped().peer)
    {
      close_peer(link.mapped().peer);
    }
  }

  void RelayPeers::send_status(const Uuid& token, const bool ready) const noexcept
  {
    try
    {
      if (const auto control{_control.lock()}; control && control->is_open())
      {
        if (control->async_send(message::RelayStatus{token, ready}))
        {
          control->close();
        }
      }
    }
    catch (const std::exception& error)
    {
      std::cerr << "Sending relay status failed: " << error.what() << '\n';
    }
  }

  void RelayPeers::forward(const message::RelayDispatch& batch)
  {
    const auto& dispatch{batch.dispatch()};
    if (!_topics.contains({dispatch.entity().application_name(), dispatch.entity().key()}))
    {
      return;
    }
    tcp::EncodedFrame frame;
    for (const auto& token : batch.targets())
    {
      const auto link{_links.find(token)};
      if (link == _links.end() || !link->second.captain || !link->second.peer
        || !matches(link->second.topic, dispatch))
      {
        continue;
      }
      auto session{link->second.peer->session};
      if (!frame)
      {
        frame = tcp::encode_frame_shared(dispatch);
      }
      if (session->async_send(frame))
      {
        session->close();
      }
    }
    if (batch.deliver_local())
    {
      _deliver(dispatch);
    }
  }
}
