# Raft 改造日志

## 2026-11-26 迁移前

以 `e0859cb51af1d887c05f7f657be75d73afa3c55f` 为基线。Cipple++ 按 `(application, key)` 管理全量写入、增量与删除，提供查询、实时订阅和持久回放。服务端用一个 I/O 线程处理会话，通过 `StorageWriter` 将写入串行提交到 SQLite。客户端可连接不同端点，并将持久订阅的事件和游标保存在自己的 SQLite 库中。 [基线架构](https://github.com/aynls/Cipple-pp/blob/e0859cb51af1d887c05f7f657be75d73afa3c55f/doc/architecture.md) · [基线协议](https://github.com/aynls/Cipple-pp/blob/e0859cb51af1d887c05f7f657be75d73afa3c55f/doc/behavior.md)

任意节点都能接受写入，通过本地事务保存实体、`delivery_log` 和各目标节点的待确认记录。事务成功后即可向发布者返回。接收节点持久化后应答 `SyncResponse`，发送节点按会话和请求 UUID 匹配应答，再批量标记 `ack`。默认 `mesh` 下，接收节点也会为其他目标登记待发记录。`star` 由写入节点向各目标发送。分区两侧可独立提交，恢复后按 `(HLC, 源节点 ID, 实体 UUID)` 排序选出当前值。 [基线写入](https://github.com/aynls/Cipple-pp/blob/e0859cb51af1d887c05f7f657be75d73afa3c55f/server/src/server.cpp) · [基线补发](https://github.com/aynls/Cipple-pp/blob/e0859cb51af1d887c05f7f657be75d73afa3c55f/server/src/replication/replication.cpp)

离线目标的待确认版本留在各相关发送者的 SQLite 中。分页、窗口和退避限制每轮调度的工作量。积压仍随离线期间的写入增长，未确认的版本也会影响历史回收。持久订阅游标基于各副本自己的 `delivery_log.sequence`。客户端按端点保存游标，切换端点后从该副本的保留历史继续，按实体 UUID 去重。 [基线 SQLite 存储](https://github.com/aynls/Cipple-pp/blob/e0859cb51af1d887c05f7f657be75d73afa3c55f/server/src/storage/sqlite_storage.cpp) · [基线订阅存储](https://github.com/aynls/Cipple-pp/blob/e0859cb51af1d887c05f7f657be75d73afa3c55f/client/src/subscription/subscription_store.cpp)

## Windows 构建方案

基线使用 CMake、Ninja 和 MSYS2 UCRT64。Boost.Asio `1.91.0` 和 SQLite `3.53.0` 来自同一个 UCRT64 安装，GoogleTest `1.15.2` 由 CMake 按哈希下载。开发环境改用 Windows x64 MSVC。vcpkg 的 `2026.10.16` registry 提交 `9eec002df1cd109f8f104cd966a763daf9751489` 提供 Boost.Asio `1.92.0`、SQLite `3.53.4#2`、GoogleTest `1.18.0` 和 NuRaft `3.0.0`。使用 `x64-windows-static`，由同一套编译器构建项目和第三方库。 [基线 CMake](https://github.com/aynls/Cipple-pp/blob/e0859cb51af1d887c05f7f657be75d73afa3c55f/CMakeLists.txt) · [vcpkg 版本基线](https://github.com/microsoft/vcpkg/blob/9eec002df1cd109f8f104cd966a763daf9751489/versions/baseline.json)

NuRaft 作为服务端的嵌入式共识库。vcpkg 校验源码并应用其 Windows 打包补丁。包内使用独立 Asio，Windows 节点间 Raft RPC 使用普通 TCP。现有应用协议使用 Boost.Asio。先用 MSVC 跑通当前 CTest、独立进程验收和示例，再单独验证 NuRaft 的三节点选主、复制和失去多数派时的提交行为。 [NuRaft port](https://github.com/microsoft/vcpkg/tree/9eec002df1cd109f8f104cd966a763daf9751489/ports/nuraft) · [NuRaft 构建说明](https://github.com/eBay/NuRaft/blob/v3.0.0/README.md#how-to-build)

## Raft 设计

Leader 将写入命令提交到 Raft 日志，在多数派持久化且本地状态机应用完毕后返回成功。Follower 引导客户端使用 Leader，重试沿用请求 UUID。各副本按提交顺序将命令应用到 SQLite，同一事务更新配置、请求去重结果、订阅事件和已应用日志位置。线性一致的读取需要当前任期的多数派确认，并等待本地应用进度到达读取位置。

Raft 持久化任期、投票、成员配置和日志。落后节点优先追赶日志，日志已压缩时先安装快照，再补快照后的条目。快照覆盖状态机恢复所需的数据和位置。订阅回放沿集群一致的版本推进，客户端换节点可沿同一位置续传。订阅历史按自身的保留规则管理，游标超出保留范围时需向客户端明确返回恢复所需的信息。旧副本的本地游标和数据导入策略需结合实际迁移验证。

## 2026-11-26 Windows 构建验证

使用 MSVC `19.50.35719.0`、vcpkg `2026-09-26` 和 `x64-windows-static` 安装了 22 个包。Release 和 Debug 均完成构建，各通过 79 项 CTest。Release 通过 15 项独立进程验收和三节点持久订阅示例。构建验证中修正了基准程序的时间转换表达式，写线程测试的后台任务改为持有自己的同步状态。发生过卡死的 Debug 测试连续运行 20 次均通过。 [复现命令](delivery.md)

NuRaft `3.0.0` 的 Windows 包已生成 `nuraft.lib`。该包编译时关闭 TLS，生成的 `NuRaftConfig.cmake` 仍因 `DISABLE_OPENSSL` 为空而查找 OpenSSL。验证程序直接从 `NuRaftTargets.cmake` 导入 `NuRaft::static_lib`。三节点均应用初始条目，终止 Leader 后剩余两节点选出新 Leader 并应用下一条写入，最后只剩一个节点时在 3.5 秒观察窗口内未应用新提案。该验证使用上游示例的内存状态机。

## Raft 代码改造

服务端通过 NuRaft 提交发布和查询。应用 SQLite 事务保存配置、请求去重记录、分发事件和已应用位置。Raft 日志库持久化任期、投票、成员与压缩边界的任期。订阅继续从任意副本读取已经应用的提交，并可用集群游标续传。 [架构](architecture.md) · [部署与数据文件](delivery.md)

改造期间，Windows Release 已构建，三节点进程用例通过了换主与失去多数派的场景。首次快照走到 Windows 目录刷盘时失败，后续代码改为刷写文件并原子发布快照完成标记。收尾阶段按要求结束验证，最终提交未重新构建和运行测试。
