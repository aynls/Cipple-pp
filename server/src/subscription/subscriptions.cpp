#include "subscriptions.hpp"

#include <algorithm>
#include <iostream>
#include <stdexcept>

#include "protocol/subscription/dispatch_request.hpp"
#include "protocol/subscription/relay_messages.hpp"
#include "server.hpp"
#include "storage/sqlite_storage.hpp"
#include "protocol/frame_codec.hpp"
#include "tcp/session.hpp"

namespace cipple::server
{
  namespace
  {
    using entity::EntityMessage;
    constexpr auto kDeliveryTimeout{std::chrono::seconds{2}};
    constexpr auto kLinkTimeout{std::chrono::seconds{3}};
    constexpr auto kRetryDelay{std::chrono::seconds{5}};
    constexpr std::size_t kBranchMembers{8};
    constexpr std::size_t kMinimumRelayTargets{2};
    constexpr std::size_t kMinimumRelayBytes{1024};
    constexpr std::size_t kRecoveryQueueLimit{1024};
    constexpr std::size_t kRecoverySendBatch{64};

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

  Subscriptions::Subscriptions(storage::SqliteStorage& reader, const RelayRecoveryOptions recovery) noexcept
    : _reader{reader}
      , _bytes_per_second{static_cast<double>(recovery.bytes_per_second)}
      , _burst_bytes{static_cast<double>(recovery.burst_bytes)}
      , _tokens{_burst_bytes}
      , _refill_at{Clock::now()}
  {
  }

  void Subscriptions::attach(const ConnectionId id, std::shared_ptr<tcp::Session> session, std::string address)
  {
    _connections.emplace(id, Connection{std::move(session), {}, std::move(address)});
  }

  void Subscriptions::advertise(const ConnectionId id, const std::uint16_t port, const std::uint16_t slots)
  {
    auto& connection{_connections.at(id)};
    if (connection.relay || !connection.subscriptions.empty())
    {
      throw std::invalid_argument{"relay advertisement must precede subscriptions"};
    }
    connection.relay = true;
    connection.port = port;
    connection.slots = slots;
  }

  void Subscriptions::detach(const ConnectionId id) noexcept
  {
    const auto connection{_connections.find(id)};
    if (connection == _connections.end())
    {
      return;
    }
    auto& subscriptions{connection->second.subscriptions};
    while (!subscriptions.empty())
    {
      unsubscribe(id, subscriptions.begin()->first);
    }
    _connections.erase(connection);
  }

  void Subscriptions::stop() noexcept
  {
    _stopping = true;
    while (!_connections.empty())
    {
      disconnect(_connections.begin()->first);
    }
    _rounds.clear();
    _recovery_queue.clear();
    _prepared.reset();
    _recovery_due.reset();
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
    unsubscribe(id, key);
    auto& connection{_connections.at(id)};
    const auto start{cursor ? *cursor : _reader.journal_watermark()};
    const auto [entry, inserted] = connection.subscriptions.emplace(
      std::move(key), Subscription{start, cursor.has_value(), {}, ++_next_subscription});
    if (cursor)
    {
      enqueue_replay(id, entry->first, start);
    }
  }

  void Subscriptions::enable_relay(const ConnectionId id, const ConfigKey& key)
  {
    auto* sub{find_subscription(id, key)};
    if (sub && sub->durable && _connections.at(id).relay)
    {
      join_branch(id, key, *sub);
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
        auto& subscription{entry->second};
        cancel_replay(id, entry->first, subscription.cursor);
        recheck_recovery_head(id, subscription);
        _recovery_waiters -= subscription.waiting;
        try
        {
          leave_branch(id, entry->first, subscription);
        }
        catch (const std::exception& error)
        {
          std::cerr << "Relay branch update failed: " << error.what() << '\n';
        }
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
    if (Clock::now() >= it->second.pending->deadline)
    {
      return;
    }
    if (!success)
    {
      disconnect(id);
      return;
    }
    auto& subscription{it->second};
    if (const auto round{_rounds.find(reply_id)}; round != _rounds.end())
    {
      for (auto& target : round->second.targets)
      {
        if (target.id == id && target.generation == subscription.generation)
        {
          target.acknowledged = true;
          break;
        }
      }
      if (std::ranges::all_of(round->second.targets, &RoundTarget::acknowledged))
      {
        _rounds.erase(round);
      }
    }
    subscription.cursor = subscription.pending->sequence;
    subscription.pending.reset();
    subscription.retry = false;
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
          if (!subscription.pending && !subscription.queued && !subscription.waiting)
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

  Subscriptions::Subscription* Subscriptions::find_subscription(const ConnectionId id,
                                                                const ConfigKey& key) noexcept
  {
    const auto connection{_connections.find(id)};
    if (connection == _connections.end())
    {
      return nullptr;
    }
    const auto entry{connection->second.subscriptions.find(key)};
    return entry == connection->second.subscriptions.end() ? nullptr : &entry->second;
  }

  bool Subscriptions::send_control(const ConnectionId id, const message::Message& message) const noexcept
  {
    try
    {
      const auto connection{_connections.find(id)};
      return connection != _connections.end() && !connection->second.session->async_send(message);
    }
    catch (const std::exception& error)
    {
      std::cerr << "Relay control send failed: " << error.what() << '\n';
      return false;
    }
  }

  bool Subscriptions::can_captain(const ConnectionId id, const ConfigKey& key, const Clock::time_point now) noexcept
  {
    const auto& connection{_connections.at(id)};
    return connection.slots > connection.used_slots && !connection.address.empty()
      && find_subscription(id, key)->captain_after <= now;
  }

  void Subscriptions::join_branch(const ConnectionId id, const ConfigKey& key, Subscription& subscription)
  {
    auto& branches{_branches[key]};
    const auto now{Clock::now()};
    const bool candidate{can_captain(id, key, now)};
    auto selected{branches.end()};
    for (auto entry{branches.begin()}; entry != branches.end(); ++entry)
    {
      const auto& branch{entry->second};
      // Reserve a place for a captain in a branch that currently has none.
      const auto limit{kBranchMembers + (branch.captain != 0 || candidate)};
      if (branch.members.size() < limit)
      {
        if (selected == branches.end())
        {
          selected = entry;
        }
        if ((branch.captain == 0) == candidate)
        {
          selected = entry;
          break;
        }
      }
    }
    if (selected == branches.end())
    {
      selected = branches.try_emplace(++_next_branch).first;
    }
    selected->second.members.insert(id);
    subscription.branch = selected->first;
    maintain_branch(key, selected->second, now);
  }

  void Subscriptions::leave_branch(const ConnectionId id, const ConfigKey& key, Subscription& subscription)
  {
    if (!subscription.branch)
    {
      return;
    }
    auto topic{_branches.find(key)};
    auto entry{topic->second.find(subscription.branch)};
    auto& branch{entry->second};
    if (subscription.link)
    {
      drop_link(*subscription.link);
    }
    if (branch.captain == id)
    {
      retire_captain(key, branch);
    }
    branch.members.erase(id);
    if (branch.members.empty())
    {
      topic->second.erase(entry);
      if (topic->second.empty())
      {
        _branches.erase(topic);
      }
    }
    else if (!_stopping)
    {
      maintain_branch(key, branch, Clock::now());
    }
  }

  void Subscriptions::maintain_branch(const ConfigKey& key, Branch& branch, const Clock::time_point now)
  {
    if (branch.captain)
    {
      const auto& captain{_connections.at(branch.captain)};
      if (captain.used_slots == captain.slots && std::ranges::none_of(branch.members, [&](const auto id)
        {
          return find_subscription(id, key)->link.has_value();
        }))
      {
        branch.captain = 0;
        ++branch.epoch;
      }
    }
    if (!branch.captain)
    {
      const auto captain{std::ranges::find_if(branch.members, [&](const auto id)
      {
        return can_captain(id, key, now);
      })};
      if (captain == branch.members.end())
      {
        return;
      }
      branch.captain = *captain;
      ++branch.epoch;
    }
    auto& captain{_connections.at(branch.captain)};
    for (const auto id : branch.members)
    {
      if (captain.used_slots == captain.slots)
      {
        break;
      }
      auto& subscription{*find_subscription(id, key)};
      if (id == branch.captain || subscription.link || subscription.link_after > now)
      {
        continue;
      }
      const auto token{Uuid::generate()};
      _links.emplace(token, Link{key, branch.captain, id, LinkPhase::kAuthorizing, now + kLinkTimeout});
      ++captain.used_slots;
      subscription.link = token;
      if (!send_control(branch.captain, message::RelayRoute{token, key.first, key.second, {}, 0}))
      {
        drop_link(token, now + kRetryDelay);
      }
    }
  }

  void Subscriptions::retire_captain(const ConfigKey& key, Branch& branch)
  {
    find_subscription(branch.captain, key)->captain_after = Clock::now() + kRetryDelay;
    branch.captain = 0;
    ++branch.epoch;
    for (const auto id : branch.members)
    {
      auto& subscription{*find_subscription(id, key)};
      if (subscription.link)
      {
        drop_link(*subscription.link);
      }
      subscription.link_after = {};
    }
  }

  void Subscriptions::drop_link(const Uuid token, const Clock::time_point retry_after) noexcept
  {
    auto entry{_links.extract(token)};
    if (entry.empty())
    {
      return;
    }
    const auto& link{entry.mapped()};
    --_connections.at(link.captain).used_slots;
    if (auto* subscription{find_subscription(link.member, link.key)})
    {
      subscription->link.reset();
      subscription->link_after = retry_after;
    }
    if (!_stopping)
    {
      const message::RelayStatus revoked{token, false};
      send_control(link.captain, revoked);
      send_control(link.member, revoked);
    }
  }

  void Subscriptions::relay_status(const ConnectionId id, const Uuid& token, const bool ready)
  {
    const auto entry{_links.find(token)};
    if (entry == _links.end())
    {
      return;
    }
    auto& link{entry->second};
    if (id != link.captain && id != link.member)
    {
      return;
    }
    const auto now{Clock::now()};
    if (!ready || (link.phase != LinkPhase::kReady && now >= link.deadline))
    {
      drop_link(token, now + kRetryDelay);
    }
    else if (id == link.captain && link.phase == LinkPhase::kAuthorizing)
    {
      const auto& captain{_connections.at(id)};
      link.phase = LinkPhase::kConnecting;
      link.deadline = now + kLinkTimeout;
      if (!send_control(link.member, message::RelayRoute{
        token, link.key.first, link.key.second, captain.address, captain.port
      }))
      {
        drop_link(token, now + kRetryDelay);
      }
    }
    else if (id == link.member && link.phase == LinkPhase::kConnecting)
    {
      link.phase = LinkPhase::kReady;
      const auto& subscription{*find_subscription(id, link.key)};
      if (!subscription.retry)
      {
        recheck_recovery_head(id, subscription);
      }
    }
  }

  bool Subscriptions::needs_recovery(const ConnectionId id, const ConfigKey& key,
                                      const Subscription& subscription) const noexcept
  {
    if (subscription.retry)
    {
      return true;
    }
    if (!subscription.branch)
    {
      return false;
    }
    const auto& branch{_branches.at(key).at(subscription.branch)};
    return branch.captain != id && (!subscription.link || _links.at(*subscription.link).phase != LinkPhase::kReady);
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
    return !_ready.empty() || (_recovery_due && *_recovery_due <= Clock::now());
  }

  std::optional<Subscriptions::Clock::time_point> Subscriptions::recovery_due() const noexcept
  {
    return _recovery_due;
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
    try
    {
      if (_recovery_due && *_recovery_due <= Clock::now())
      {
        drain_recovery();
      }
    }
    catch (const std::exception& error)
    {
      std::cerr << "Subscription recovery failed: " << error.what() << '\n';
    }
  }

  void Subscriptions::relay_group(const ConfigKey& key, const std::int64_t cursor,
                                  const Recipients& recipients, const EntityMessage& version,
                                  const std::optional<EntityMessage>& base, const std::int64_t sequence,
                                  const Uuid& reply_id, tcp::EncodedFrame& direct, Recipients& sent)
  {
    const auto topic{_branches.find(key)};
    if (topic == _branches.end())
    {
      return;
    }
    std::map<std::uint64_t, std::vector<RoundTarget>> batches;
    for (const auto id : recipients)
    {
      const auto* sub{find_subscription(id, key)};
      if (sub && sub->cursor == cursor && !sub->pending && !sub->queued && !sub->waiting && sub->link
        && !needs_recovery(id, key, *sub))
      {
        batches[sub->branch].push_back({id, sub->generation});
      }
    }
    for (auto& [branch_id, targets] : batches)
    {
      if (targets.size() < kMinimumRelayTargets)
      {
        continue;
      }
      if (!direct)
      {
        direct = tcp::encode_frame_shared(message::DispatchRequest{reply_id, version, sequence, base});
      }
      if (direct->size() < kMinimumRelayBytes)
      {
        return;
      }
      auto& branch{topic->second.at(branch_id)};
      std::vector<Uuid> links;
      links.reserve(targets.size());
      for (const auto& target : targets)
      {
        links.push_back(*find_subscription(target.id, key)->link);
      }
      const auto captain{branch.captain};
      auto& captain_sub{*find_subscription(captain, key)};
      const bool deliver_local{recipients.contains(captain) && captain_sub.cursor == cursor
        && !captain_sub.pending && !captain_sub.retry && !captain_sub.queued && !captain_sub.waiting};
      const auto attempt{Uuid::generate()};
      tcp::EncodedFrame frame;
      try
      {
        frame = tcp::encode_frame_shared(message::RelayDispatch{
          message::DispatchRequest{attempt, version, sequence, base},
          deliver_local, std::move(links)
        });
      }
      catch (const std::length_error&)
      {
        continue;
      }
      if (!send_frame(*_connections.at(captain).session, frame))
      {
        retire_captain(key, branch);
        maintain_branch(key, branch, Clock::now());
        continue;
      }
      const auto deadline{Clock::now() + kDeliveryTimeout};
      const PendingDelivery pending{attempt, sequence, deadline};
      for (const auto& target : targets)
      {
        find_subscription(target.id, key)->pending = pending;
        sent.insert(target.id);
      }
      if (deliver_local)
      {
        captain_sub.pending = pending;
        sent.insert(captain);
      }
      _rounds.emplace(attempt, Round{key, branch_id, branch.epoch, deadline, std::move(targets)});
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
      Recipients relayed;
      relay_group(key, cursor, recipients, version, base, sequence, reply_id, durable_frame, relayed);
      for (const auto id : recipients)
      {
        if (relayed.contains(id))
        {
          continue;
        }
        const auto connection{_connections.find(id)};
        if (connection == _connections.end())
        {
          continue;
        }
        auto& subscriptions{connection->second.subscriptions};
        const auto entry{subscriptions.find(key)};
        if (entry == subscriptions.end() || entry->second.cursor != cursor || entry->second.pending
          || entry->second.queued || entry->second.waiting)
        {
          continue;
        }
        auto& subscription{entry->second};
        if (needs_recovery(id, key, subscription))
        {
          enqueue_recovery(id, key, subscription);
          continue;
        }
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

  void Subscriptions::enqueue_recovery(const ConnectionId id, const ConfigKey& key, Subscription& sub)
  {
    if (_recovery_queue.size() == kRecoveryQueueLimit)
    {
      sub.waiting = true;
      ++_recovery_waiters;
      return;
    }
    _recovery_queue.push_back({id, key, sub.generation, sub.cursor});
    sub.queued = true;
    if (!_recovery_due)
    {
      _recovery_due = Clock::now();
    }
  }

  void Subscriptions::recheck_recovery_head(const ConnectionId id, const Subscription& subscription) noexcept
  {
    if (subscription.queued && !_recovery_queue.empty())
    {
      const auto& head{_recovery_queue.front()};
      if (head.id == id && head.generation == subscription.generation)
      {
        _recovery_due = Clock::now();
      }
    }
  }

  void Subscriptions::refill_recovery_queue()
  {
    if (!_recovery_waiters || _recovery_queue.size() == kRecoveryQueueLimit)
    {
      return;
    }
    for (auto& [id, connection] : _connections)
    {
      for (auto& [key, sub] : connection.subscriptions)
      {
        if (sub.waiting)
        {
          enqueue_recovery(id, key, sub);
          sub.waiting = false;
          --_recovery_waiters;
          if (!_recovery_waiters || _recovery_queue.size() == kRecoveryQueueLimit)
          {
            return;
          }
        }
      }
    }
  }

  void Subscriptions::drain_recovery()
  {
    for (std::size_t processed{0}; !_recovery_queue.empty() && processed < kRecoverySendBatch; ++processed)
    {
      const auto& task{_recovery_queue.front()};
      const auto id{task.id};
      auto* sub{find_subscription(id, task.key)};
      if (!sub || sub->generation != task.generation || sub->cursor != task.cursor || sub->pending)
      {
        _recovery_queue.pop_front();
        _prepared.reset();
        continue;
      }
      if (!needs_recovery(id, task.key, *sub))
      {
        enqueue_replay(id, task.key, task.cursor);
      }
      else
      {
        try
        {
          if (!_prepared)
          {
            auto events{_reader.journal_after(task.key.first, task.key.second, task.cursor, 1)};
            if (!events.empty())
            {
              const auto& [sequence, version] = events.front();
              const auto attempt{Uuid::generate()};
              _prepared = PreparedRecovery{attempt, sequence, tcp::encode_frame_shared(
                message::DispatchRequest{attempt, version, sequence, delivery_base(version)}
              )};
            }
          }
          if (_reader.journal_floor(task.key.first, task.key.second) > task.cursor)
          {
            _prepared.reset();
            disconnect(id);
            sub = nullptr;
          }
          if (_prepared)
          {
            const auto now{Clock::now()};
            _tokens = std::min(_burst_bytes,
              _tokens + std::chrono::duration<double>{now - _refill_at}.count() * _bytes_per_second);
            _refill_at = now;
            const auto bytes{static_cast<double>(_prepared->frame->size())};
            if (_tokens < bytes)
            {
              _recovery_due = now + std::chrono::ceil<Clock::duration>(
                std::chrono::duration<double>{(bytes - _tokens) / _bytes_per_second});
              refill_recovery_queue();
              return;
            }
            if (send_frame(*_connections.at(id).session, _prepared->frame))
            {
              _tokens -= bytes;
              sub->pending = PendingDelivery{_prepared->attempt, _prepared->sequence, Clock::now() + kDeliveryTimeout};
            }
            else
            {
              disconnect(id);
              sub = nullptr;
            }
          }
        }
        catch (const std::exception& error)
        {
          std::cerr << "Subscription recovery failed: " << error.what() << '\n';
          disconnect(id);
          sub = nullptr;
        }
      }
      if (sub)
      {
        sub->queued = false;
      }
      _recovery_queue.pop_front();
      _prepared.reset();
    }
    refill_recovery_queue();
    _recovery_due = _recovery_queue.empty() ? std::nullopt : std::optional{Clock::now()};
  }

  void Subscriptions::finish_rounds(const Clock::time_point now)
  {
    for (auto entry{_rounds.begin()}; entry != _rounds.end();)
    {
      if (entry->second.deadline > now)
      {
        ++entry;
        continue;
      }
      const auto round{_rounds.extract(entry++)};
      const auto& batch{round.mapped()};
      const auto topic{_branches.find(batch.key)};
      if (topic == _branches.end())
      {
        continue;
      }
      const auto branch{topic->second.find(batch.branch)};
      if (branch == topic->second.end() || branch->second.epoch != batch.epoch)
      {
        continue;
      }
      std::size_t healthy{0};
      std::size_t missed{0};
      for (const auto& target : batch.targets)
      {
        const auto* sub{find_subscription(target.id, batch.key)};
        if (sub && sub->generation == target.generation)
        {
          ++healthy;
          missed += !target.acknowledged;
        }
      }
      if (missed > healthy / 2)
      {
        retire_captain(batch.key, branch->second);
        maintain_branch(batch.key, branch->second, now);
      }
    }
  }

  void Subscriptions::expire() noexcept
  {
    const auto now{Clock::now()};
    try
    {
      finish_rounds(now);
      for (auto it{_links.begin()}; it != _links.end();)
      {
        const auto& [token, link] = *it++;
        if (link.phase != LinkPhase::kReady && link.deadline <= now)
        {
          drop_link(token, now + kRetryDelay);
        }
      }
      for (auto& [key, branches] : _branches)
      {
        for (auto& [id, branch] : branches)
        {
          maintain_branch(key, branch, now);
        }
      }
    }
    catch (const std::exception& error)
    {
      std::cerr << "Relay branch update failed: " << error.what() << '\n';
    }
    for (auto it{_connections.begin()}; it != _connections.end();)
    {
      auto& [id, connection] = *it++;
      for (auto& [key, sub] : connection.subscriptions)
      {
        if (!sub.pending || sub.pending->deadline > now)
        {
          continue;
        }
        if (!connection.relay)
        {
          disconnect(id);
          break;
        }
        sub.pending.reset();
        sub.retry = true;
        try
        {
          enqueue_recovery(id, key, sub);
        }
        catch (const std::exception& error)
        {
          std::cerr << "Enqueueing subscription recovery failed: " << error.what() << '\n';
          disconnect(id);
          break;
        }
      }
    }
  }
}
