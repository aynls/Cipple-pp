#pragma once
#include <boost/asio/ip/tcp.hpp>

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>

#include "uuid.hpp"

namespace cipple::tcp
{
  class Session;
}

namespace cipple::message
{
  class DispatchRequest;
  class RelayDispatch;
  class RelayRoute;
  class Message;
}

namespace cipple::client
{
  // One set of peer links per Server session, confined to the client I/O thread.
  class RelayPeers final : public std::enable_shared_from_this<RelayPeers>
  {
  public:
    using Topic = std::pair<std::string, std::string>;
    using DeliveryHandler = std::function<void(const message::DispatchRequest&)>;

    RelayPeers(boost::asio::io_context& context, std::weak_ptr<tcp::Session> control, DeliveryHandler deliver);
    ~RelayPeers() noexcept;

    void start(bool ipv6, std::optional<std::uint16_t> port, std::uint16_t slots);
    void stop() noexcept;
    void track(Topic topic, bool durable);
    void route(const message::RelayRoute& request);
    void revoke(const Uuid& token) noexcept;
    void forward(const message::RelayDispatch& batch);

  private:
    struct Peer;

    struct Link
    {
      Topic topic;
      bool captain;
      std::shared_ptr<Peer> peer;
    };

    void accept();
    void start_session(const std::shared_ptr<Peer>& peer, boost::asio::ip::tcp::socket socket);
    void start_deadline(const std::shared_ptr<Peer>& peer);
    void on_peer(const std::shared_ptr<Peer>& peer, tcp::Session& session, const message::Message& message);
    void close_peer(const std::shared_ptr<Peer>& peer) noexcept;
    void send_status(const Uuid& token, bool ready) const noexcept;

    boost::asio::io_context& _context;
    std::weak_ptr<tcp::Session> _control;
    DeliveryHandler _deliver;
    std::optional<boost::asio::ip::tcp::acceptor> _acceptor;
    std::uint16_t _slots{0};
    std::uint64_t _next_peer{0};
    std::set<Topic> _topics;
    std::map<Uuid, Link> _links;
    std::map<std::uint64_t, std::shared_ptr<Peer>> _incoming;
  };
}
