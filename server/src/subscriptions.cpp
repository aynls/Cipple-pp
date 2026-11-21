#include "subscriptions.hpp"

#include <algorithm>
#include <iostream>
#include <stdexcept>

#include "message/dispatch_request.hpp"
#include "storage/storage.hpp"
#include "tcp/frame_codec.hpp"
#include "tcp/session.hpp"

namespace cipple::server
{
  namespace
  {
    using entity::EntityMessage;
    constexpr auto kDeliveryTimeout = std::chrono::seconds{2};

    bool send_frame(tcp::Session& session, const tcp::EncodedFrame& frame) noexcept
    {
      try
      {
        return !session.async_send(frame);
      }
      catch (const std::exception& error)
      {
        std::cerr << "Subscription send failed: " << error.what() << '\n';
        return false;
      }
    }
  }

  Subscriptions::Subscriptions(storage::Storage& reader) noexcept : _reader{reader}
  {
  }

  void Subscriptions::attach(const ConnectionId id, std::shared_ptr<tcp::Session> session)
  {
    _connections.emplace(id, Connection{std::move(session), {}});
  }

  void Subscriptions::detach(const ConnectionId id) noexcept
  {
    if (const auto connection = _connections.find(id); connection != _connections.end())
    {
      for (const auto& [key, subscription] : connection->second.subscriptions)
      {
        remove_route(id, key, subscription);
      }
      _connections.erase(connection);
    }
  }

  void Subscriptions::stop() noexcept
  {
    while (!_connections.empty())
    {
      disconnect(_connections.begin()->first);
    }
  }

  void Subscriptions::disconnect(const ConnectionId id) noexcept
  {
    if (const auto connection = _connections.find(id); connection != _connections.end())
    {
      const auto session = connection->second.session;
      detach(id);
      session->close();
    }
  }

  void Subscriptions::subscribe(const ConnectionId id, ConfigKey key, const std::optional<std::int64_t> cursor)
  {
    auto& subscriptions = _connections.at(id).subscriptions;
    const auto [entry, inserted] = subscriptions.try_emplace(std::move(key));
    if (inserted)
    {
      _by_key[entry->first].insert(id);
    }
    else if (entry->second.cursor)
    {
      cancel_replay(id, entry->first, *entry->second.cursor);
    }
    entry->second = Subscription{.cursor = cursor};
    if (cursor)
    {
      enqueue_replay(id, entry->first, *cursor);
    }
  }

  void Subscriptions::unsubscribe(const ConnectionId id, const ConfigKey& key) noexcept
  {
    const auto connection = _connections.find(id);
    if (connection != _connections.end())
    {
      auto& subscriptions = connection->second.subscriptions;
      if (const auto entry = subscriptions.find(key); entry != subscriptions.end())
      {
        remove_route(id, entry->first, entry->second);
        subscriptions.erase(entry);
      }
    }
  }

  void Subscriptions::remove_route(const ConnectionId id, const ConfigKey& key,
                                  const Subscription& subscription) noexcept
  {
    if (const auto route = _by_key.find(key); route != _by_key.end())
    {
      route->second.erase(id);
      if (route->second.empty())
      {
        _by_key.erase(route);
      }
    }
    if (subscription.cursor)
    {
      cancel_replay(id, key, *subscription.cursor);
    }
  }

  void Subscriptions::acknowledge(const ConnectionId id, const Uuid& reply_id, const bool success)
  {
    auto& subscriptions = _connections.at(id).subscriptions;
    const auto it = std::ranges::find_if(subscriptions, [&](const auto& entry) {
      return entry.second.pending && entry.second.pending->reply_id == reply_id;
    });
    if (it == subscriptions.end())
    {
      return;
    }
    if (!success)
    {
      disconnect(id);
      return;
    }
    auto& subscription = it->second;
    subscription.cursor = subscription.pending->sequence;
    subscription.pending.reset();
    enqueue_replay(id, it->first, *subscription.cursor);
  }

  template<typename Visit>
  void Subscriptions::visit_subscribers(const ConfigKey& key, Visit visit) noexcept
  {
    const auto route = _by_key.find(key);
    if (route == _by_key.end())
    {
      return;
    }
    for (auto next = route->second.begin(); next != route->second.end();)
    {
      const auto id = *next++;
      auto& connection = _connections.at(id);
      if (!visit(id, connection, connection.subscriptions.at(key)))
      {
        // Disconnect erases this recipient and may remove the key's last route.
        const bool has_more = next != route->second.end();
        disconnect(id);
        if (!has_more)
        {
          return;
        }
      }
    }
  }

  void Subscriptions::publish_live(const ConfigKey& key, EntityMessage version,
                                   std::optional<EntityMessage> base) noexcept
  {
    tcp::EncodedFrame frame;
    bool encoded = false;
    visit_subscribers(key, [&](ConnectionId, Connection& connection, const Subscription& subscription) {
      if (subscription.cursor)
      {
        return true;
      }
      if (!encoded)
      {
        encoded = true;
        try
        {
          frame = tcp::encode_frame_shared(message::DispatchRequest{
            Uuid::generate(), "", std::move(version), 0, std::move(base),
          });
        }
        catch (const std::exception& error)
        {
          std::cerr << "Live subscription dispatch failed: " << error.what() << '\n';
        }
      }
      return frame && send_frame(*connection.session, frame);
    });
  }

  void Subscriptions::wake(const ConfigKey& key) noexcept
  {
    visit_subscribers(key, [&](const ConnectionId id, Connection&, const Subscription& subscription) {
      if (subscription.cursor && !subscription.pending)
      {
        try
        {
          enqueue_replay(id, key, *subscription.cursor);
        }
        catch (const std::exception& error)
        {
          std::cerr << "Subscription wake failed: " << error.what() << '\n';
          return false;
        }
      }
      return true;
    });
  }

  void Subscriptions::enqueue_replay(const ConnectionId id, const ConfigKey& key, const std::int64_t cursor)
  {
    _ready[key][cursor].insert(id);
  }

  void Subscriptions::cancel_replay(const ConnectionId id, const ConfigKey& key, const std::int64_t cursor) noexcept
  {
    const auto topic = _ready.find(key);
    if (topic == _ready.end())
    {
      return;
    }
    if (const auto group = topic->second.find(cursor); group != topic->second.end())
    {
      group->second.erase(id);
      if (group->second.empty())
      {
        topic->second.erase(group);
      }
    }
    if (topic->second.empty())
    {
      _ready.erase(topic);
    }
  }

  bool Subscriptions::has_ready() const noexcept
  {
    return !_ready.empty();
  }

  void Subscriptions::replay_ready() noexcept
  {
    while (!_ready.empty())
    {
      // A failed dispatch may synchronously detach a connection from other ready groups.
      const auto topic = _ready.extract(_ready.begin());
      for (const auto& [cursor, recipients] : topic.mapped())
      {
        replay_group(topic.key(), cursor, recipients);
      }
    }
  }

  void Subscriptions::replay_group(const ConfigKey& key, const std::int64_t cursor,
                                   const Recipients& recipients) noexcept
  {
    try
    {
      auto entries = _reader.journal_after(key.first, key.second, cursor, 1);
      if (entries.empty())
      {
        return;
      }
      auto& [sequence, version] = entries.front();
      std::optional<EntityMessage> base;
      if (const auto* delta = std::get_if<EntityMessage::IncrementalUpdate>(&version.payload()))
      {
        auto stored = _reader.find_message(delta->base_message_uuid);
        if (!stored || !std::holds_alternative<EntityMessage::Update>(stored->payload())
            || stored->application_name() != key.first || stored->key() != key.second)
        {
          throw std::runtime_error{"stored incremental update has no full base"};
        }
        base.emplace(std::move(*stored));
      }
      const auto reply_id = Uuid::generate();
      const auto frame = tcp::encode_frame_shared(message::DispatchRequest{
        reply_id, "", std::move(version), sequence, std::move(base),
      });
      for (const auto id : recipients)
      {
        const auto connection = _connections.find(id);
        if (connection == _connections.end())
        {
          continue;
        }
        auto& subscriptions = connection->second.subscriptions;
        const auto entry = subscriptions.find(key);
        if (entry == subscriptions.end() || entry->second.cursor != cursor || entry->second.pending)
        {
          continue;
        }
        if (send_frame(*connection->second.session, frame))
        {
          entry->second.pending = PendingDelivery{reply_id, sequence, Clock::now() + kDeliveryTimeout};
        }
        else
        {
          disconnect(id);
        }
      }
    }
    catch (const std::exception& error)
    {
      std::cerr << "Subscription replay failed: " << error.what() << '\n';
      for (const auto id : recipients)
      {
        disconnect(id);
      }
    }
  }

  void Subscriptions::expire() noexcept
  {
    const auto now = Clock::now();
    for (auto it = _connections.begin(); it != _connections.end();)
    {
      const auto& [id, connection] = *it++;
      if (std::ranges::any_of(connection.subscriptions, [&](const auto& entry) {
        const auto& pending = entry.second.pending;
        return pending && pending->deadline <= now;
      }))
      {
        disconnect(id);
      }
    }
  }
}
