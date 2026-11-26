#include "subscriptions.hpp"

#include <algorithm>
#include <iostream>
#include <stdexcept>

#include "protocol/subscription/dispatch_request.hpp"
#include "storage/sqlite_storage.hpp"
#include "protocol/frame_codec.hpp"
#include "tcp/session.hpp"

namespace cipple::server
{
  namespace
  {
    using entity::EntityMessage;
    constexpr auto kDeliveryTimeout{std::chrono::seconds{2}};

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

  Subscriptions::Subscriptions(storage::SqliteStorage& reader) noexcept : _reader{reader}
  {
  }

  void Subscriptions::attach(const ConnectionId id, std::shared_ptr<tcp::Session> session)
  {
    _connections.emplace(id, Connection{std::move(session), {}});
  }

  void Subscriptions::detach(const ConnectionId id) noexcept
  {
    if (const auto connection{_connections.find(id)}; connection != _connections.end())
    {
      for (const auto& [key, subscription] : connection->second.subscriptions)
      {
        cancel_replay(id, key, subscription.cursor);
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
    if (const auto connection{_connections.find(id)}; connection != _connections.end())
    {
      const auto session{connection->second.session};
      detach(id);
      session->close();
    }
  }

  void Subscriptions::subscribe(const ConnectionId id, ConfigKey key, const std::optional<std::int64_t> cursor)
  {
    auto& subscriptions{_connections.at(id).subscriptions};
    if (const auto old{subscriptions.find(key)}; old != subscriptions.end())
    {
      cancel_replay(id, old->first, old->second.cursor);
    }
    const auto start{cursor ? *cursor : _reader.journal_watermark()};
    const auto [entry, inserted] = subscriptions.insert_or_assign(std::move(key), Subscription{start, cursor.has_value()});
    if (cursor)
    {
      enqueue_replay(id, entry->first, start);
    }
  }

  void Subscriptions::unsubscribe(const ConnectionId id, const ConfigKey& key) noexcept
  {
    const auto connection{_connections.find(id)};
    if (connection != _connections.end())
    {
      auto& subscriptions{connection->second.subscriptions};
      if (const auto entry{subscriptions.find(key)}; entry != subscriptions.end())
      {
        cancel_replay(id, entry->first, entry->second.cursor);
        subscriptions.erase(entry);
      }
    }
  }

  void Subscriptions::acknowledge(const ConnectionId id, const Uuid& reply_id, const bool success)
  {
    auto& subscriptions{_connections.at(id).subscriptions};
    const auto it{
      std::ranges::find_if(subscriptions, [&](const auto& entry)
      {
        return entry.second.pending && entry.second.pending->reply_id == reply_id;
      })
    };
    if (it == subscriptions.end())
    {
      return;
    }
    if (!success)
    {
      disconnect(id);
      return;
    }
    auto& subscription{it->second};
    subscription.cursor = subscription.pending->sequence;
    subscription.pending.reset();
    enqueue_replay(id, it->first, subscription.cursor);
  }

  void Subscriptions::wake() noexcept
  {
    for (auto next{_connections.begin()}; next != _connections.end();)
    {
      const auto& [id, connection] = *next++;
      try
      {
        for (const auto& [key, subscription] : connection.subscriptions)
        {
          if (!subscription.pending)
          {
            enqueue_replay(id, key, subscription.cursor);
          }
        }
      }
      catch (const std::exception& error)
      {
        std::cerr << "Subscription wake failed: " << error.what() << '\n';
        disconnect(id);
      }
    }
  }

  void Subscriptions::enqueue_replay(const ConnectionId id, const ConfigKey& key, const std::int64_t cursor)
  {
    _ready[key][cursor].insert(id);
  }

  void Subscriptions::cancel_replay(const ConnectionId id, const ConfigKey& key, const std::int64_t cursor) noexcept
  {
    const auto topic{_ready.find(key)};
    if (topic == _ready.end())
    {
      return;
    }
    if (const auto group{topic->second.find(cursor)}; group != topic->second.end())
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

  std::optional<EntityMessage> Subscriptions::delivery_base(const EntityMessage& version) const
  {
    const auto* delta{std::get_if<EntityMessage::IncrementalUpdate>(&version.payload())};
    if (!delta)
    {
      return std::nullopt;
    }
    auto base{_reader.find_message(delta->base_message_uuid)};
    if (!base || !std::holds_alternative<EntityMessage::Update>(base->payload())
      || base->application_name() != version.application_name() || base->key() != version.key())
    {
      throw std::runtime_error{"stored incremental update has no full base"};
    }
    return std::move(*base);
  }

  void Subscriptions::replay_ready() noexcept
  {
    decltype(_ready) batch;
    batch.swap(_ready);
    for (const auto& [key, groups] : batch)
    {
      for (const auto& [cursor, recipients] : groups)
      {
        replay_group(key, cursor, recipients);
      }
    }
  }

  void Subscriptions::replay_group(const ConfigKey& key, const std::int64_t cursor,
                                   const Recipients& recipients) noexcept
  {
    try
    {
      auto entries{_reader.journal_after(key.first, key.second, cursor, 1)};
      // Recheck after the query in case apply compacted the prefix concurrently.
      if (_reader.journal_floor(key.first, key.second) > cursor)
      {
        for (const auto id : recipients)
        {
          disconnect(id);
        }
        return;
      }
      if (entries.empty())
      {
        return;
      }
      const auto& [sequence, version] = entries.front();
      const auto base{delivery_base(version)};
      const auto reply_id{Uuid::generate()};
      tcp::EncodedFrame durable_frame;
      tcp::EncodedFrame live_frame;
      for (const auto id : recipients)
      {
        const auto connection{_connections.find(id)};
        if (connection == _connections.end())
        {
          continue;
        }
        auto& subscriptions{connection->second.subscriptions};
        const auto entry{subscriptions.find(key)};
        if (entry == subscriptions.end() || entry->second.cursor != cursor || entry->second.pending)
        {
          continue;
        }
        auto& subscription{entry->second};
        auto& frame{subscription.durable ? durable_frame : live_frame};
        if (!frame)
        {
          frame = tcp::encode_frame_shared(message::DispatchRequest{
            reply_id, version, subscription.durable ? sequence : 0, base,
          });
        }
        if (!send_frame(*connection->second.session, frame))
        {
          disconnect(id);
        }
        else if (subscription.durable)
        {
          subscription.pending = PendingDelivery{reply_id, sequence, Clock::now() + kDeliveryTimeout};
        }
        else
        {
          subscription.cursor = sequence;
          enqueue_replay(id, key, sequence);
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
    const auto now{Clock::now()};
    for (auto it{_connections.begin()}; it != _connections.end();)
    {
      const auto& [id, connection] = *it++;
      if (std::ranges::any_of(connection.subscriptions, [&](const auto& entry)
      {
        const auto& pending{entry.second.pending};
        return pending && pending->deadline <= now;
      }))
      {
        disconnect(id);
      }
    }
  }
}
