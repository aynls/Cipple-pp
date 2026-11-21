# 黑盒验收的命令与观测面

验收脚本通过公开命令驱动 Cipple++ 进程。代理在本机转发 TCP，时钟偏移由进程参数控制。报告、数据库和临时文件写到 `--output`
，默认使用系统临时目录。

## 服务端

```text
cipple_server <port> <database> --bind 127.0.0.1 --id <正整数>
    --peers <id@ip:port,id@ip:port> --overlay star|mesh
    --clock-offset-ms <整数，闭区间 [-86400000, 86400000]>
```

- 成功监听时向标准输出打印 `Listening on 127.0.0.1:<port>`，并刷新。
- `--peers` 里的地址是本节点主动拨号的地址。测试把它们设成代理，因此节点不能绕过该地址去直连对端真实端口。
- `--overlay star` 时，只有消息的源节点负责向自己的 peer 推送；`mesh` 才继续转发。
- `--clock-offset-ms` 只影响本进程 HybridClock 的物理毫秒，不得要求测试去改操作系统时间。
- 集群节点的 `--id` 必须是正整数，并且和 `--peers` 里的 id 不冲突。
- 进程被直接终止后，用同一个数据库路径重新启动，必须恢复未确认复制；监听端口若暂时不能复用，测试会换端口并把代理的上游改过去，peer
  字符串保持不变。

## 客户端

```text
cipple_client [--timeout-ms MS] [--verbose] [--request-id UUID]
    ENDPOINT put APP KEY VALUE
cipple_client [--timeout-ms MS] ENDPOINT get|get-base|delete APP KEY
cipple_client [--timeout-ms MS] [--request-id UUID]
    ENDPOINT incremental APP KEY BASE_UUID add_entry|remove_entry VALUE
cipple_client --state DB [--timeout-ms MS] ENDPOINT subscribe APP KEY
cipple_client [--verbose] [--timeout-ms MS] ENDPOINT,ENDPOINT get APP KEY
```

- `ENDPOINT` 可以是 `port` 或 `ip:port`，多个端点按逗号分隔。
- 故障转移按给定顺序尝试，连接被拒绝后转到下一个。同一列表两次运行应落到同一个第一存活端点。
- `--request-id` 是写入的消息 UUID。负载相同的重试返回成功且不新增版本；负载不同必须拒绝，原版本保持不变。
- `--state` 把订阅游标和已见消息 UUID 存在调用者给出的数据库里。重连同一端点时从该游标补发，包括已经被更新的删除遮蔽掉的增量。
- 当前值打到标准输出，历史版本的 UUID 打到标准错误，输出都要及时刷新。删除的当前值打印 `<deleted>`。
- 不带 `--state` 的订阅是会话内实时推送，订阅游标为 −1。退订后不再推这个键。

## 确认帧，必须能被代理单独丢掉

复制头注释里的契约是：对端把 SyncRequest 持久化之后，才回 SyncResponse；源节点只在收到结果为成功或重复（0 或 1）时，才把
`ack.acknowledged` 置 1。

SyncResponse 是一个独立帧。代理按请求 UUID 识别要丢弃的回执。

- SyncRequest 使用类型 2，RPC UUID 后是实体和发送节点 ID。实体以消息 UUID 开头，增量同步还携带完整基底。
- SyncResponse 使用类型 3，携带同一个 RPC UUID 和一个结果字节。

拒绝结果（2）不是持久化确认，丢掉它不能代替上面的测试。

## 订阅回执

带 `resume_after` 的订阅按 `delivery_log.sequence` 逐条补发。订阅者必须以 DispatchResponse 回执；约 2
秒没有回执就断开该会话。用同一个游标重连必须重放未确认的那条，确认之后不得再发一次。

## 可以直接读的库表

测试在服务运行时用 SQLite 只读模式打开数据库。这些表就是观测面，改名或改变语义会让“未确认”和“删除之后增量是否还在”无法黑盒判定：

- `message(uuid, operation_type, last_update, last_update_server_id, atomic_operation, value, base_message_uuid)`
- `ack(message_uuid, node_id, acknowledged)`，`acknowledged` 为 0/1
- `delivery_log(sequence, message_uuid, application_name, key)`，`sequence` 是追加顺序

可见值仍以 get 为准：最新版本是删除时 get 失败并打印 `Configuration not found`。增量是否被恢复，以 `delivery_log`
和订阅补发为准，不能只看 get。

## 和当前复制实现的对应关系

`server/src/replication.cpp` 中的 `Replication` 使用 RPC UUID 关联每次发送，实体 UUID 用于去重。收到匹配的 SyncResponse
且结果为成功或重复（0 或 1）后，源节点将该实体加入批量确认队列。会话仍在时，丢失回复后的第一次重发等待是 2
秒。丢弃确认的代理会立刻拆掉连接并拒绝后续连接，未完成的尝试回到待发队列。

`--overlay mesh` 时，已经持久化该版本的接收节点继续向其余 peer 传播。源节点在副本落盘后退出并保持死亡，只恢复其余节点之间的连接，持有副本的节点仍应把版本送到此前被隔离开的节点。
`--overlay star` 不转发这条路径。

可执行文件是官方 CMake 构建的 `cipple_server` 和 `cipple_client`，由 `--server`、`--client` 传入。验收不再把全部 `.cpp`
重新编译进临时目录。二进制缺失、无法监听或 `--peers` 起不来时，对应用例记为阻塞或失败，并附上进程的原始输出；任一失败或阻塞都会使进程退出码非零。单节点用例不依赖对端拨号。
