# Cipple++ 架构

Cipple++ 按 `(application, key)` 管理配置，支持全量写入、引用完整基底的增量和删除。NuRaft 负责选主与复制，SQLite 保存各节点的 Raft 日志、配置状态与订阅历史。应用继续通过原有 TCP 协议访问配置和订阅。 [改造日志](raft-migration.md)

## 提交与查询

客户端可提供多个应用端点。Follower 或暂时无法确认请求的节点返回 `RetryResponse`，客户端在请求期限内轮换端点，并保留请求 UUID 和内容。Leader 将命令写入日志，多数派持久化之后才可提交。状态机在同一 SQLite 事务中保存配置、订阅事件和请求结果，推进已应用日志位置。成功响应在事务提交之后发出。提交后丢失响应可以重试相同 UUID 和内容。 [请求入口](../server/src/server.cpp) · [客户端](../client/src/client.cpp)

SQLite 按日志顺序应用所有变更。新集群以日志索引作为版本和分发序号。增量在提交时验证指定的完整基底，删除作为一个版本提交。请求结果按 UUID 和内容摘要持久去重，拒绝相同 UUID 的不同内容。GET 也经过 Raft 提交，在应用该条目时读取配置。Leader 必须能完成一次多数派提交才能回答，因此查询会消耗日志和磁盘 I/O。 [配置状态机](../server/src/raft/configuration_machine.cpp)

应用 I/O 线程负责会话、请求分发和订阅。Raft 在独立线程上处理共识、应用日志和快照。应用 I/O 线程使用独立的读连接查询已应用的订阅历史。写入以及等待发送的查询结果设有容量上限。 [Raft 节点](../server/src/raft/raft_node.cpp) · [订阅](../server/src/subscription/subscriptions.cpp)

## 副本恢复与日志保留

每个节点分别保存任期、投票、固定成员配置、Raft 日志和状态机数据。重启时从持久化位置恢复。Follower 缺失的日志仍在时，由 Leader 追加这些条目。日志前缀已经压缩时，Leader 分块发送快照，再追加快照之后的条目。快照包含配置、仍可回放的事件、回放边界和请求去重结果。日志压缩随提交进度推进，离线副本可以在恢复连接后从快照追赶。 [Raft 持久化](../server/src/raft/raft_persistence.cpp) · [配置状态机](../server/src/raft/configuration_machine.cpp)

每个键最多保留最近 128 条订阅事件。完整基底在仍被保留的增量引用时继续保存。去重记录独立于事件载荷保留，数量随请求数增长。

## 订阅

任意节点可以提供实时订阅和持久回放，事件来自该副本已经应用的提交前缀。当副本落后时，订阅要等它应用到相应位置。实时订阅从建订阅时的本地已应用位置之后开始。持久订阅用同一集群的分发序号在副本间续传。服务端每个订阅保留一条待确认投递，客户端把事件和游标写入同一事务后回执。 [订阅路由](../server/src/subscription/subscriptions.cpp) · [客户端订阅存储](../client/src/subscription/subscription_store.cpp)

游标早于该键的保留边界时，服务端返回可用的最小游标。客户端收到此响应后可显式选择恢复位置。正在追赶的订阅落后到保留窗口外会断开，重连时检查保留边界。 [行为与协议](behavior.md)

## 部署边界

静态 Raft 集群的每个节点配置同一组成员 ID 与 RPC 地址。应用端口和 Raft 端口分别配置。三节点可在一个节点故障时继续提交。多数派不可用时，发布者和查询方会收到可重试响应或超时。Windows 的 NuRaft RPC 使用普通 TCP，Raft 端口应部署在可信的节点间网络。 [Windows 部署与验证](delivery.md)
