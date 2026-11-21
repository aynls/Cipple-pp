#include "replication.hpp"

#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

#include <algorithm>
#include <atomic>
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
#include "storage/storage_writer.hpp"
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

    constexpr std::size_t kMaxInFlight = 64;       // 每条会话上未回执的 SyncRequest 上限
    constexpr std::size_t kMaxStaged = 2048;       // 待发、在途、待确认合计上限，不含长期失败项
    constexpr std::size_t kMaxDeferred = 64;       // 拒绝后退避的消息上限
    constexpr std::size_t kMaxUndeliverable = 64;  // 记住的超帧消息上限
    constexpr std::size_t kMaxAwaitingAck = 256;   // 已收到回执、尚未证实落盘的消息上限
    constexpr std::size_t kAckBatch = 32;          // 一次 FULL 事务里合并的确认数
    constexpr int kMaxPendingPages = 4;            // 一轮泵最多越过的索引页
    constexpr std::int64_t kFetchBatch = 256;      // 单页 pending id 数
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

    struct AckPersist
    {
      std::vector<Uuid> ids;
      std::atomic<bool> committed{false};
    };

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
    struct AwaitingAck
    {
      bool submitted = false;   // 已进入一次尚未回调的确认批次
    };

    std::unordered_set<Uuid> staged;                // backlog ∪ deferred ∪ in_flight ∪ awaiting
    std::unordered_set<Uuid> undeliverable;         // 超帧：有界记忆，超出后靠游标跳过
    std::optional<storage::PendingCursor> scan_cursor;
    std::unordered_map<Uuid, AwaitingAck> awaiting;
    std::deque<Uuid> ack_ready;
    bool ack_batch_open = false;
    bool ack_needs_retry = false;
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

  std::shared_ptr<Replication> Replication::start(asio::io_context& context, storage::Storage& reads,
                                                  storage::StorageWriter& writer, const std::int32_t node_id,
                                                  std::vector<PeerAddress> peers)
  {
    auto replication = std::shared_ptr<Replication>{
      new Replication{context, reads, writer, node_id, std::move(peers)}};
    replication->open_peers();
    return replication;
  }

  Replication::Replication(asio::io_context& context, storage::Storage& reads, storage::StorageWriter& writer,
                           const std::int32_t node_id, std::vector<PeerAddress> peers)
    : _context{context}
      , _reads{reads}
      , _writer{writer}
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
      // 确认落盘优先于继续装载。低流量的小批量由 poll 周期刷新，这里只处理凑满的批次和失败重试。
      if (peer.ack_needs_retry || peer.ack_ready.size() >= kAckBatch)
      {
        flush_acks(peer);
      }
      if (peer.in_flight.size() < kMaxInFlight && peer.awaiting.size() < kMaxAwaitingAck
          && fresh_resident(peer) < kMaxStaged)
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

  std::size_t Replication::fresh_resident(const Peer& peer) noexcept
  {
    std::size_t count = peer.awaiting.size();
    for (const auto& outbound : peer.backlog)
    {
      if (outbound.attempts == 0)
      {
        ++count;
      }
    }
    for (const auto& flight : peer.in_flight)
    {
      if (flight.second.outbound.attempts == 0)
      {
        ++count;
      }
    }
    return count;
  }

  std::size_t Replication::parked_retries(const Peer& peer) noexcept
  {
    std::size_t count = peer.deferred.size();
    for (const auto& outbound : peer.backlog)
    {
      if (outbound.attempts > 0)
      {
        ++count;
      }
    }
    return count;
  }

  void Replication::stage_pending(Peer& peer)
  {
    const auto occupied = fresh_resident(peer);
    if (occupied >= kMaxStaged || peer.awaiting.size() >= kMaxAwaitingAck)
    {
      return;
    }
    const auto need = std::min(kMaxStaged - occupied, static_cast<std::size_t>(kFetchBatch));
    std::size_t admitted = 0;
    for (int page = 0; page < kMaxPendingPages && admitted < need; ++page)
    {
      const auto rows = _reads.pending_ids_after(peer.id, peer.scan_cursor, kFetchBatch);
      if (rows.empty())
      {
        peer.scan_cursor.reset();
        break;
      }
      std::optional<storage::PendingCursor> advanced;
      bool filled = false;
      for (const auto& row : rows)
      {
        const storage::PendingCursor position{row.last_update, row.last_update_server_id, row.uuid};
        if (peer.staged.contains(row.uuid) || peer.undeliverable.contains(row.uuid))
        {
          advanced = position;
          continue;
        }
        if (admitted >= need)
        {
          filled = true;
          break;
        }
        peer.staged.insert(row.uuid);
        try
        {
          peer.backlog.push_back(Peer::Outbound{row.uuid, 0});
        }
        catch (...)
        {
          // 保持 staged ⊆ 各队列：入队失败就撤销标记，让下轮拉取重新装载。
          peer.staged.erase(row.uuid);
          throw;
        }
        advanced = position;
        ++admitted;
      }
      if (advanced)
      {
        peer.scan_cursor = advanced;
      }
      if (filled)
      {
        break;
      }
      if (rows.size() < static_cast<std::size_t>(kFetchBatch))
      {
        peer.scan_cursor.reset();
        break;
      }
    }
  }

  void Replication::release_ack(Peer& peer, const Uuid& id) noexcept
  {
    if (peer.awaiting.erase(id) == 0)
    {
      return;
    }
    peer.staged.erase(id);
  }

  void Replication::flush_acks(Peer& peer)
  {
    if (_stopped || peer.ack_batch_open || peer.ack_ready.empty())
    {
      return;
    }
    std::vector<Uuid> batch;
    batch.reserve(std::min(kAckBatch, peer.ack_ready.size()));
    while (!peer.ack_ready.empty() && batch.size() < kAckBatch)
    {
      auto id = std::move(peer.ack_ready.front());
      peer.ack_ready.pop_front();
      const auto found = peer.awaiting.find(id);
      if (found == peer.awaiting.end() || found->second.submitted)
      {
        continue;
      }
      found->second.submitted = true;
      batch.push_back(std::move(id));
    }
    if (batch.empty())
    {
      return;
    }
    auto write = std::make_shared<AckPersist>();
    write->ids = std::move(batch);
    peer.ack_batch_open = true;
    const auto node_id = peer.id;
    bool queued = false;
    try
    {
      queued = _writer.submit(_context,
        [write, node_id](storage::Storage& storage) {
          storage.mark_acked_batch(node_id, write->ids);
          write->committed.store(true, std::memory_order_release);
        },
        [weak = weak_from_this(), &peer, write] {
          if (const auto self = weak.lock())
          {
            self->on_acks_persisted(peer, write->ids, write->committed.load(std::memory_order_acquire));
          }
        });
    }
    catch (...)
    {
      queued = false;
    }
    if (queued)
    {
      peer.ack_needs_retry = false;
      return;
    }
    peer.ack_batch_open = false;
    peer.ack_needs_retry = true;
    for (auto it = write->ids.rbegin(); it != write->ids.rend(); ++it)
    {
      const auto found = peer.awaiting.find(*it);
      if (found == peer.awaiting.end())
      {
        continue;
      }
      found->second.submitted = false;
      peer.ack_ready.push_front(*it);
    }
  }

  void Replication::prove_acks(Peer& peer)
  {
    std::vector<Uuid> submitted;
    submitted.reserve(peer.awaiting.size());
    for (const auto& [id, state] : peer.awaiting)
    {
      if (state.submitted)
      {
        submitted.push_back(id);
      }
    }
    if (submitted.empty())
    {
      return;
    }
    const auto durable = _reads.acknowledged_among(peer.id, submitted);
    for (const auto& id : durable)
    {
      release_ack(peer, id);
    }
  }

  void Replication::on_acks_persisted(Peer& peer, std::vector<Uuid> ids, const bool committed)
  {
    peer.ack_batch_open = false;
    if (_stopped)
    {
      return;
    }
    try
    {
      if (!committed)
      {
        // 写失败只重试本地确认，不把实体放回发送队列。下一次 poll 再提交。
        peer.ack_needs_retry = true;
        for (auto it = ids.rbegin(); it != ids.rend(); ++it)
        {
          const auto found = peer.awaiting.find(*it);
          if (found == peer.awaiting.end())
          {
            continue;
          }
          found->second.submitted = false;
          peer.ack_ready.push_front(*it);
        }
        return;
      }
      for (const auto& id : ids)
      {
        release_ack(peer, id);
      }
      flush_acks(peer);
      pump(peer);
    }
    catch (const std::exception& error)
    {
      peer.ack_needs_retry = true;
      fail(peer, "acknowledge", error);
    }
  }

  void Replication::drain(Peer& peer)
  {
    while (peer.session && peer.session->is_open() && !peer.write_blocked
           && peer.awaiting.size() < kMaxAwaitingAck && peer.in_flight.size() < kMaxInFlight
           && !peer.backlog.empty())
    {
      auto outbound = std::move(peer.backlog.front());
      peer.backlog.pop_front();
      try
      {
        const auto message_uuid = outbound.message_uuid;
        auto entity = _reads.find_message(message_uuid);
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
          if (auto found = _reads.find_message(delta->base_message_uuid))
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
          // 超帧集合有上限。记不住时不占 staged，游标已经越过它，下一圈才会再试一次。
          peer.staged.erase(message_uuid);
          if (peer.undeliverable.size() < kMaxUndeliverable)
          {
            peer.undeliverable.insert(message_uuid);
            std::cerr << "Replication: peer " << peer.id << ": message " << message_uuid.to_string()
                      << " exceeds the frame limit and stays pending\n";
          }
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
    // 超时不再插回队首。能留下的重试有上限，超出的卸下装载资格，把发送容量让给还没试过的版本。
    for (auto& outbound : expired)
    {
      ++outbound.attempts;
      const auto id = outbound.message_uuid;
      try
      {
        if (parked_retries(peer) < kMaxDeferred)
        {
          peer.backlog.push_back(std::move(outbound));
        }
        else
        {
          peer.staged.erase(id);
        }
      }
      catch (...)
      {
        peer.staged.erase(id);
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
      for (auto& outbound : rescued)
      {
        const auto id = outbound.message_uuid;
        try
        {
          if (outbound.attempts == 0 || parked_retries(peer) < kMaxDeferred)
          {
            peer.backlog.push_back(std::move(outbound));
          }
          else
          {
            peer.staged.erase(id);
          }
        }
        catch (...)
        {
          peer.staged.erase(id);
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
        // kRejected 永不标记 ack。退避集合有上限；放不下就卸下装载资格，游标继续往后。
        auto outbound = attempt->second.outbound;
        ++outbound.attempts;
        const auto entity_id = outbound.message_uuid;
        const auto not_before = Clock::now() + retry_delay(outbound.attempts);
        peer.in_flight.erase(attempt);
        if (peer.deferred.size() < kMaxDeferred)
        {
          peer.deferred.push_back({std::move(outbound), not_before});
          if (!peer.deferred_wake || not_before < *peer.deferred_wake)
          {
            peer.deferred_wake = not_before;
          }
        }
        else
        {
          peer.staged.erase(entity_id);
        }
        ++peer.rejections_since_report;
      }
      else
      {
        // 成功或重复：离开发送窗口，但在本机 ack 提交或读库证实之前不得重新装载。
        const auto entity_id = attempt->second.outbound.message_uuid;
        if (!peer.awaiting.contains(entity_id))
        {
          peer.ack_ready.push_back(entity_id);
          peer.awaiting.emplace(entity_id, Peer::AwaitingAck{});
        }
        peer.in_flight.erase(attempt);
        peer.failure_reported = false;
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
      prove_acks(peer);
      flush_acks(peer);
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
