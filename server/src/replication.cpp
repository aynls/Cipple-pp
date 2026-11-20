#include "replication.hpp"

#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

#include <algorithm>
#include <chrono>
#include <deque>
#include <exception>
#include <iostream>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "entity/entity_message.hpp"
#include "message/heartbeat_request.hpp"
#include "message/heartbeat_response.hpp"
#include "message/message.hpp"
#include "message/message_type.hpp"
#include "message/result.hpp"
#include "message/sync_request.hpp"
#include "message/sync_response.hpp"
#include "storage/storage.hpp"
#include "tcp/connector.hpp"
#include "tcp/session.hpp"
#include "uuid.hpp"

namespace cipple::server
{
  namespace
  {
    namespace asio = boost::asio;
    using entity::EntityMessage;
    using message::MessageType;
    using Clock = std::chrono::steady_clock;

    constexpr std::size_t kMaxInFlight = 64;    // 每条会话上未回执的 SyncRequest 上限
    constexpr std::size_t kMaxStaged = 2048;    // 从 SQLite 暂存进内存的消息上限
    constexpr std::int64_t kFetchBatch = 256;   // 单次 pending_for_node 拉取上限
    constexpr auto kPollInterval = std::chrono::milliseconds{250};
    constexpr auto kHeartbeatInterval = std::chrono::seconds{2};
    constexpr unsigned kMaxMissedBeats = 3;
    constexpr auto kMinRetryDelay = std::chrono::seconds{2};
    constexpr auto kMaxRetryDelay = std::chrono::seconds{30};

    // 每条消息的指数退避：2s、4s、8s、16s、30s 封顶。
    Clock::duration retry_delay(const unsigned attempts) noexcept
    {
      const auto shift = std::min(attempts, 4u);
      return std::min(kMinRetryDelay * (1u << shift), kMaxRetryDelay);
    }

  }

  struct Replication::Peer
  {
    struct Outbound
    {
      Uuid message_uuid;
      unsigned attempts = 0;   // 驱动该消息的重发超时/退避
    };

    struct Deferred
    {
      Outbound outbound;
      Clock::time_point not_before;
    };

    struct InFlight
    {
      Outbound outbound;
      Clock::time_point deadline;
    };

    Peer(asio::io_context& context, const std::int32_t id, asio::ip::tcp::endpoint endpoint)
      : id{id}
        , endpoint{std::move(endpoint)}
        , poll_timer{context}
        , heartbeat_timer{context}
        , retry_timer{context}
    {
    }

    const std::int32_t id;
    const asio::ip::tcp::endpoint endpoint;
    std::shared_ptr<tcp::Connector> connector;
    std::shared_ptr<tcp::Session> session;          // 仅连接期间非空
    std::deque<Outbound> backlog;                   // 立即可发，按 pending 序
    std::deque<Deferred> deferred;                  // 被拒后按退避时间挂起
    std::unordered_map<Uuid, InFlight> in_flight;   // 已发出、等待 SyncResponse
    std::unordered_set<Uuid> staged;                // backlog ∪ deferred ∪ in_flight
    std::unordered_set<Uuid> undeliverable;         // 超帧长：永久留在 pending，不再装载
    std::optional<Clock::time_point> deferred_wake;
    bool write_blocked = false;
    bool pump_posted = false;
    bool awaiting_pong = false;
    unsigned missed_beats = 0;
    std::size_t writes_completed = 0;
    std::size_t writes_at_last_beat = 0;
    std::size_t rejections_since_report = 0;
    bool rejection_reported = false;
    bool failure_reported = false;
    asio::steady_timer poll_timer;
    asio::steady_timer heartbeat_timer;
    asio::steady_timer retry_timer;
  };

  std::shared_ptr<Replication> Replication::start(asio::io_context& context, storage::Storage& storage,
                                                  const std::int32_t node_id, std::vector<PeerAddress> peers)
  {
    auto replication = std::shared_ptr<Replication>{
      new Replication{context, storage, node_id, std::move(peers)}};
    replication->open_peers();
    return replication;
  }

  Replication::Replication(asio::io_context& context, storage::Storage& storage, const std::int32_t node_id,
                           std::vector<PeerAddress> peers)
    : _context{context}
      , _storage{storage}
      , _node_id{node_id}
  {
    std::unordered_set<std::int32_t> seen;
    for (const auto& peer : peers)
    {
      if (peer.id == node_id || !seen.insert(peer.id).second)
      {
        continue;
      }
      _peers.push_back(std::make_unique<Peer>(context, peer.id, peer.endpoint));
    }
  }

  Replication::~Replication() noexcept
  {
    stop();
  }

  void Replication::stop() noexcept
  {
    if (_stopped)
    {
      return;
    }
    _stopped = true;
    for (const auto& peer : _peers)
    {
      try
      {
        peer->poll_timer.cancel();
        peer->heartbeat_timer.cancel();
        peer->retry_timer.cancel();
        // Connector 先置 kStopped 再关会话：close 回调里不会触发 on_disconnected。
        // open_peers 中途抛异常时靠后注册的 peer 还没有 connector。
        if (peer->connector)
        {
          peer->connector->stop();
        }
      }
      catch (const std::exception&)
      {
      }
      peer->session.reset();
    }
  }

  void Replication::notify()
  {
    for (const auto& peer : _peers)
    {
      request_pump(*peer);
    }
  }

  void Replication::open_peers()
  {
    for (const auto& entry : _peers)
    {
      Peer& peer = *entry;
      const tcp::ConnectorHandlers handlers{
        .on_connected = [weak = weak_from_this(), &peer](std::shared_ptr<tcp::Session> session) {
          if (const auto self = weak.lock())
          {
            self->on_connected(peer, std::move(session));
          }
        },
        .on_message = [weak = weak_from_this(), &peer](tcp::Session& session,
                                                       std::unique_ptr<message::Message> message) {
          if (const auto self = weak.lock())
          {
            self->on_peer_message(peer, session, *message);
          }
        },
        .on_disconnected = [weak = weak_from_this(), &peer](const boost::system::error_code reason) {
          if (const auto self = weak.lock())
          {
            self->on_disconnected(peer, reason);
          }
        },
      };
      peer.connector = tcp::Connector::start(_context, peer.endpoint, handlers);
      peer.poll_timer.expires_after(kPollInterval);
      peer.poll_timer.async_wait([weak = weak_from_this(), &peer](const boost::system::error_code error) {
        if (!error)
        {
          if (const auto self = weak.lock())
          {
            self->on_poll(peer);
          }
        }
      });
      peer.heartbeat_timer.expires_after(kHeartbeatInterval);
      peer.heartbeat_timer.async_wait([weak = weak_from_this(), &peer](const boost::system::error_code error) {
        if (!error)
        {
          if (const auto self = weak.lock())
          {
            self->on_heartbeat(peer);
          }
        }
      });
      // 启动即装载 SQLite 里遗留的 pending 行（崩溃恢复、断连期间积累）。
      request_pump(peer);
    }
  }

  void Replication::request_pump(Peer& peer) noexcept
  {
    if (_stopped || peer.pump_posted)
    {
      return;
    }
    peer.pump_posted = true;
    try
    {
      asio::post(_context, [weak = weak_from_this(), &peer] {
        if (const auto self = weak.lock())
        {
          peer.pump_posted = false;
          self->pump(peer);
        }
      });
    }
    catch (const std::exception&)
    {
      // post 失败只是丢一次合并调度，poll 兜底；清标记让下次 notify 重试。
      peer.pump_posted = false;
    }
  }

  void Replication::pump(Peer& peer) noexcept
  {
    if (_stopped)
    {
      return;
    }
    try
    {
      // 只在待发队列掏空时才查存储：高频写入路径上每 kFetchBatch 条至多一次查询。
      if (peer.backlog.empty() && peer.staged.size() < kMaxStaged
          && peer.in_flight.size() < kMaxInFlight)
      {
        stage_pending(peer);
      }
      drain(peer);
      peer.failure_reported = false;
    }
    catch (const std::exception& error)
    {
      fail(peer, "pump", error);
    }
    rearm_retry_timer(peer);
  }

  void Replication::stage_pending(Peer& peer)
  {
    const auto room = static_cast<std::int64_t>(kMaxStaged - peer.staged.size());
    for (const auto& uuid : _storage.pending_ids_for_node(peer.id, std::min<std::int64_t>(kFetchBatch, room)))
    {
      if (peer.staged.contains(uuid) || peer.undeliverable.contains(uuid))
      {
        continue;
      }
      peer.staged.insert(uuid);
      try
      {
        peer.backlog.push_back(Peer::Outbound{uuid, 0});
      }
      catch (...)
      {
        // 保持 staged ⊆ 各队列：入队失败就撤销标记，让下轮拉取重新装载。
        peer.staged.erase(uuid);
        throw;
      }
    }
  }

  void Replication::drain(Peer& peer)
  {
    while (peer.session && peer.session->is_open() && !peer.write_blocked
           && peer.in_flight.size() < kMaxInFlight && !peer.backlog.empty())
    {
      auto outbound = std::move(peer.backlog.front());
      peer.backlog.pop_front();
      try
      {
        const auto message_uuid = outbound.message_uuid;
        auto entity = _storage.find_message(message_uuid);
        if (!entity)
        {
          peer.staged.erase(message_uuid);
          continue;
        }
        const auto timeout = retry_delay(outbound.attempts);

        // 增量更新携带存储中原始的完整基底，对端据此恢复缺失或被回收的基底。
        std::optional<EntityMessage> base;
        if (const auto* delta = std::get_if<EntityMessage::IncrementalUpdate>(&entity->payload()))
        {
          if (auto found = _storage.find_message(delta->base_message_uuid))
          {
            base = std::move(*found);
          }
        }

        // A new correlation ID identifies this attempt, while the entity ID deduplicates retries.
        const auto request_uuid = Uuid::generate();
        const message::SyncRequest request{request_uuid, std::move(*entity), _node_id, std::move(base)};
        const auto error = peer.session->async_send(request,
          [weak = weak_from_this(), &peer](const boost::system::error_code write_error) {
            if (const auto self = weak.lock())
            {
              self->on_written(peer, write_error);
            }
          });
        if (!error)
        {
          peer.in_flight.emplace(request_uuid,
                                 Peer::InFlight{std::move(outbound), Clock::now() + timeout});
          continue;
        }
        if (error == asio::error::message_size)
        {
          // 永远装不进帧：跳过装载、保持 pending，不再纠缠这条消息。
          peer.staged.erase(message_uuid);
          peer.undeliverable.insert(message_uuid);
          std::cerr << "Replication: peer " << peer.id << ": message " << message_uuid.to_string()
                    << " exceeds the frame limit and stays pending\n";
          continue;
        }
        peer.backlog.push_front(std::move(outbound));
        if (error == asio::error::no_buffer_space)
        {
          // 背压：写队列满，等一个挂起的写完成后再继续。
          peer.write_blocked = true;
        }
        break;
      }
      catch (...)
      {
        peer.backlog.push_front(std::move(outbound));
        throw;
      }
    }
  }

  void Replication::expire_due(Peer& peer)
  {
    const auto now = Clock::now();
    for (auto it = peer.deferred.begin(); it != peer.deferred.end();)
    {
      if (it->not_before <= now)
      {
        auto outbound = std::move(it->outbound);
        it = peer.deferred.erase(it);
        try
        {
          peer.backlog.push_back(std::move(outbound));
        }
        catch (...)
        {
          peer.staged.erase(outbound.message_uuid);
        }
      }
      else
      {
        ++it;
      }
    }
    peer.deferred_wake.reset();
    for (const auto& entry : peer.deferred)
    {
      if (!peer.deferred_wake || entry.not_before < *peer.deferred_wake)
      {
        peer.deferred_wake = entry.not_before;
      }
    }

    std::vector<Peer::Outbound> expired;
    expired.reserve(peer.in_flight.size());
    for (auto it = peer.in_flight.begin(); it != peer.in_flight.end();)
    {
      if (it->second.deadline <= now)
      {
        expired.push_back(std::move(it->second.outbound));
        it = peer.in_flight.erase(it);
      }
      else
      {
        ++it;
      }
    }
    // Increase the deadline for the next attempt on a lost reply.
    for (auto it = expired.rbegin(); it != expired.rend(); ++it)
    {
      ++it->attempts;
      try
      {
        peer.backlog.push_front(std::move(*it));
      }
      catch (...)
      {
        peer.staged.erase(it->message_uuid);
      }
    }
  }

  void Replication::rearm_retry_timer(Peer& peer) noexcept
  {
    try
    {
      if (_stopped)
      {
        peer.retry_timer.cancel();
        return;
      }
      auto next = peer.deferred_wake;
      for (const auto& [uuid, entry] : peer.in_flight)
      {
        if (!next || entry.deadline < *next)
        {
          next = entry.deadline;
        }
      }
      if (!next)
      {
        peer.retry_timer.cancel();
        return;
      }
      peer.retry_timer.expires_at(*next);
      peer.retry_timer.async_wait([weak = weak_from_this(), &peer](const boost::system::error_code error) {
        if (!error)
        {
          if (const auto self = weak.lock())
          {
            self->on_retry(peer);
          }
        }
      });
    }
    catch (const std::exception&)
    {
    }
  }

  void Replication::fail(Peer& peer, const char* operation, const std::exception& error) noexcept
  {
    if (!peer.failure_reported)
    {
      peer.failure_reported = true;
      std::cerr << "Replication: peer " << peer.id << " " << operation << " failed: " << error.what()
                << '\n';
    }
  }

  void Replication::close_session(Peer& peer) noexcept
  {
    // close() 同步触发 finish -> on_disconnected，后者自己复位 peer.session
    // 并把 in-flight 收回 backlog；副本持锁保证会话在本次调用内存活。
    if (const auto session = peer.session)
    {
      session->close();
    }
  }

  void Replication::on_connected(Peer& peer, std::shared_ptr<tcp::Session> session) noexcept
  {
    peer.session = std::move(session);
    peer.write_blocked = false;
    peer.awaiting_pong = false;
    peer.missed_beats = 0;
    peer.writes_at_last_beat = peer.writes_completed;
    pump(peer);
  }

  void Replication::on_disconnected(Peer& peer, const boost::system::error_code reason) noexcept
  {
    peer.session.reset();
    peer.write_blocked = false;
    peer.awaiting_pong = false;
    peer.missed_beats = 0;
    std::cerr << "Replication: peer " << peer.id << " disconnected: " << reason.message() << '\n';
    try
    {
      // The old session's unanswered attempts return to the queue.
      std::vector<Peer::Outbound> rescued;
      rescued.reserve(peer.in_flight.size());
      for (auto& [uuid, entry] : peer.in_flight)
      {
        rescued.push_back(std::move(entry.outbound));
      }
      peer.in_flight.clear();
      for (auto it = rescued.rbegin(); it != rescued.rend(); ++it)
      {
        try
        {
          peer.backlog.push_front(std::move(*it));
        }
        catch (...)
        {
          peer.staged.erase(it->message_uuid);
        }
      }
    }
    catch (const std::exception&)
    {
      // 收容失败时 in-flight 条目留在表里，由超时机制稍后重发。
    }
    rearm_retry_timer(peer);
  }

  void Replication::on_peer_message(Peer& peer, tcp::Session& session, const message::Message& inbound) noexcept
  {
    if (peer.session.get() != &session)
    {
      return;
    }
    // 任何入站帧都证明会话双向可用。
    peer.missed_beats = 0;
    peer.awaiting_pong = false;
    try
    {
      switch (inbound.type())
      {
      case MessageType::kSyncResponse:
        on_sync_response(peer, static_cast<const message::SyncResponse&>(inbound));
        break;
      case MessageType::kHeartbeatRequest:
        // 回答对端方向的心跳：type 0 -> 1。
        if (peer.session && peer.session->is_open())
        {
          const message::HeartbeatResponse response{inbound.uuid(), true};
          static_cast<void>(peer.session->async_send(response));
        }
        break;
      default:
        // SyncRequest 由接收侧处理；第三方 AckRequest 不作为回执凭据。
        break;
      }
    }
    catch (const std::exception& error)
    {
      fail(peer, "receive", error);
    }
  }

  void Replication::on_sync_response(Peer& peer, const message::SyncResponse& response) noexcept
  {
    const auto attempt = peer.in_flight.find(response.uuid());
    if (attempt == peer.in_flight.end())
    {
      // 迟到的或对不属于本会话 uuid 的响应：绝不构成回执。
      return;
    }
    try
    {
      if (response.result() == message::Result::kRejected)
      {
        // kRejected 永不标记 ack：递增退避后挂起，稍后重发。
        auto outbound = attempt->second.outbound;
        ++outbound.attempts;
        const auto not_before = Clock::now() + retry_delay(outbound.attempts);
        peer.deferred.push_back({std::move(outbound), not_before});
        peer.in_flight.erase(attempt);
        if (!peer.deferred_wake || not_before < *peer.deferred_wake)
        {
          peer.deferred_wake = not_before;
        }
        ++peer.rejections_since_report;
      }
      else
      {
        // kSuccess / kDuplicated = 对端已持久化：先落 mark_acked，再清本地状态；
        // 标记失败时保持 staged，随后按超时重发。
        const auto entity_id = attempt->second.outbound.message_uuid;
        _storage.mark_acked(entity_id, peer.id);
        peer.failure_reported = false;
        peer.staged.erase(entity_id);
        peer.in_flight.erase(attempt);
      }
    }
    catch (const std::exception& error)
    {
      fail(peer, "acknowledge", error);
      return;
    }
    pump(peer);   // 窗口空出，立即继续
  }

  void Replication::on_written(Peer& peer, const boost::system::error_code error) noexcept
  {
    // 写完成只说明帧上了 socket，绝不构成 ack；仅用于背压恢复与心跳的进度判定。
    if (error)
    {
      return;
    }
    ++peer.writes_completed;
    if (peer.write_blocked)
    {
      peer.write_blocked = false;
      request_pump(peer);
    }
  }

  void Replication::on_poll(Peer& peer) noexcept
  {
    if (_stopped)
    {
      return;
    }
    if (peer.rejections_since_report != 0)
    {
      if (!peer.rejection_reported)
      {
        peer.rejection_reported = true;
        std::cerr << "Replication: peer " << peer.id << " rejected " << peer.rejections_since_report
                  << " sync message(s); they remain pending\n";
      }
    }
    else
    {
      peer.rejection_reported = false;
    }
    try
    {
      expire_due(peer);
    }
    catch (const std::exception& error)
    {
      fail(peer, "retry", error);
    }
    pump(peer);
    try
    {
      peer.poll_timer.expires_after(kPollInterval);
      peer.poll_timer.async_wait([weak = weak_from_this(), &peer](const boost::system::error_code error) {
        if (!error)
        {
          if (const auto self = weak.lock())
          {
            self->on_poll(peer);
          }
        }
      });
    }
    catch (const std::exception&)
    {
    }
  }

  void Replication::on_heartbeat(Peer& peer) noexcept
  {
    if (_stopped)
    {
      return;
    }
    try
    {
      if (peer.session && peer.session->is_open())
      {
        const bool progressed = peer.writes_completed != peer.writes_at_last_beat;
        peer.writes_at_last_beat = peer.writes_completed;
        if (peer.awaiting_pong)
        {
          ++peer.missed_beats;
        }
        if (peer.missed_beats >= kMaxMissedBeats)
        {
          // TCP 连着但对端收不到/回不了：判定为连通分区，关会话交给 Connector 重连。
          std::cerr << "Replication: peer " << peer.id << " missed " << peer.missed_beats
                    << " heartbeats; reconnecting\n";
          close_session(peer);
        }
        else if (!peer.awaiting_pong)
        {
          const message::HeartbeatRequest request{Uuid::generate()};
          const auto error = peer.session->async_send(request);
          if (!error)
          {
            peer.awaiting_pong = true;
          }
          else if (error != asio::error::no_buffer_space || !progressed)
          {
            // 写队列拥塞时，只有写完成也停摆才算一次心跳丢失。
            ++peer.missed_beats;
          }
        }
      }
    }
    catch (const std::exception& error)
    {
      fail(peer, "heartbeat", error);
    }
    try
    {
      peer.heartbeat_timer.expires_after(kHeartbeatInterval);
      peer.heartbeat_timer.async_wait([weak = weak_from_this(), &peer](const boost::system::error_code error) {
        if (!error)
        {
          if (const auto self = weak.lock())
          {
            self->on_heartbeat(peer);
          }
        }
      });
    }
    catch (const std::exception&)
    {
    }
  }

  void Replication::on_retry(Peer& peer) noexcept
  {
    if (_stopped)
    {
      return;
    }
    try
    {
      expire_due(peer);
    }
    catch (const std::exception& error)
    {
      fail(peer, "retry", error);
    }
    pump(peer);
  }
}
