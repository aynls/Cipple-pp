#pragma once
#include <boost/asio/ip/tcp.hpp>
#include <boost/system/error_code.hpp>

#include <cstdint>
#include <exception>
#include <memory>
#include <vector>

#include "uuid.hpp"

namespace boost::asio
{
  class io_context;
}

namespace cipple::message
{
  class Message;
  class SyncResponse;
}

namespace cipple::storage
{
  class Storage;
  class StorageWriter;
}

namespace cipple::tcp
{
  class Session;
}

namespace cipple::server
{
  // 配置层给出的一个对端：节点 id + 其监听地址（star 或 mesh 都逐条列出）。
  struct PeerAddress
  {
    std::int32_t id;
    boost::asio::ip::tcp::endpoint endpoint;
  };

  // 出站复制泵：为本节点存储层登记的每条 pending 消息向每个 peer 维护一条
  // 自动重连的 TCP 会话，按版本序推送 SyncRequest。只有同一会话里匹配的
  // SyncResponse（kSuccess/kDuplicated）才离开发送窗口，进入有界的待确认集合；
  // ack 行提交成功，或读连接已经看见 acknowledged = 1，才释放装载资格。
  // 丢包、崩溃、重连都只会让尚未确认的消息保持 pending 并被重发（at-least-once）。
  // 所有方法与回调都在 context 的单条 I/O 线程上运行。
  class Replication final : public std::enable_shared_from_this<Replication>
  {
  public:
    // peers 中忽略 id == node_id 的自身项与重复 id。reads 是读连接，
    // writer 承担 mark_acked 提交；两者由调用方持有，且必须比本对象活得
    // 更久（stop() 之后不得再触发存储访问）。
    static std::shared_ptr<Replication> start(boost::asio::io_context& context,
                                              storage::Storage& reads, storage::StorageWriter& writer,
                                              std::int32_t node_id, std::vector<PeerAddress> peers);
    ~Replication() noexcept;

    // 新消息已写入存储并登记 ack 目标后调用；合并为一次泵送调度，可高频调用。
    void notify();
    // 幂等：停掉全部定时器与会话；未确认的消息保持 pending，重启后继续。
    void stop() noexcept;

  private:
    struct Peer;

    Replication(boost::asio::io_context& context, storage::Storage& reads, storage::StorageWriter& writer,
                std::int32_t node_id, std::vector<PeerAddress> peers);

    void open_peers();
    void request_pump(Peer& peer) noexcept;
    void pump(Peer& peer) noexcept;
    void stage_pending(Peer& peer);
    void flush_acks(Peer& peer);
    void prove_acks(Peer& peer);
    void on_acks_persisted(Peer& peer, std::vector<Uuid> ids, bool committed);
    void release_ack(Peer& peer, const Uuid& id) noexcept;
    static std::size_t fresh_resident(const Peer& peer) noexcept;
    static std::size_t parked_retries(const Peer& peer) noexcept;
    void drain(Peer& peer);
    void expire_due(Peer& peer);
    void rearm_retry_timer(Peer& peer) noexcept;
    void fail(Peer& peer, const char* operation, const std::exception& error) noexcept;
    void close_session(Peer& peer) noexcept;

    void on_connected(Peer& peer, std::shared_ptr<tcp::Session> session) noexcept;
    void on_disconnected(Peer& peer, boost::system::error_code reason) noexcept;
    void on_peer_message(Peer& peer, tcp::Session& session, const message::Message& message) noexcept;
    void on_sync_response(Peer& peer, const message::SyncResponse& response) noexcept;
    void on_written(Peer& peer, boost::system::error_code error) noexcept;
    void on_poll(Peer& peer) noexcept;
    void on_heartbeat(Peer& peer) noexcept;
    void on_retry(Peer& peer) noexcept;

    boost::asio::io_context& _context;
    storage::Storage& _reads;
    storage::StorageWriter& _writer;
    std::int32_t _node_id;
    std::vector<std::unique_ptr<Peer>> _peers;
    bool _stopped = false;
  };
}
