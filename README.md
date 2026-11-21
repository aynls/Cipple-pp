# Cipple++

Cipple++ 是使用 C++23 实现的分布式配置发布订阅系统。服务按 `(application, key)` 管理全量更新、增量和删除。副本通过 TCP
同步已持久化的配置，持久订阅可以在断线、进程重启和换节点后补齐更新。

设计导读见[项目架构](doc/architecture.md)。

## 构建并运行演示

Windows 构建需要 CMake 3.28 或以上、Ninja 和支持 C++23 `std::expected` 的编译器。验证环境使用 MSYS2 UCRT64 GCC 15.2、Boost
1.91、SQLite 3.53、CMake 4.4.2 和 Ninja 1.13.2。演示与集成实验使用 Python 3。

在 MSYS2 UCRT64 终端安装依赖。

```bash
pacman -S --needed mingw-w64-ucrt-x86_64-gcc mingw-w64-ucrt-x86_64-boost mingw-w64-ucrt-x86_64-sqlite3 mingw-w64-ucrt-x86_64-cmake mingw-w64-ucrt-x86_64-ninja
```

在仓库根目录的 PowerShell 7 中设置 UCRT64 路径。把示例路径替换为安装位置。

```powershell
$env:UCRT64_ROOT = 'C:\msys64\ucrt64'
$env:PATH = "$env:UCRT64_ROOT\bin;$env:PATH"
pwsh -File scripts/demo.ps1
```

演示构建 Release 可执行文件，启动三个独立服务端进程，执行写入、离线期间的增量和删除，再让订阅客户端换节点补齐。成功时打印
`Demo passed`，数据库和日志保存在输出所示的临时目录。对已有构建可使用 `pwsh -File scripts/demo.ps1 -SkipBuild`。

运行单元测试。

```powershell
cmake --build build --parallel 6
ctest --test-dir build --output-on-failure
```

CMake 首次配置下载带 SHA256 校验的 GoogleTest 1.15.2。离线环境可通过 `-DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=<已解包源码目录>`
指定同版本源码。

## 启动集群

首次启动时指定数据库路径。服务端在空库中创建所需的表和索引，重启时使用同一路径恢复数据。

在三个终端分别启动副本。每个副本使用自己的 SQLite 文件和唯一正整数节点 ID，同一集群的节点配置同一组成员。

```powershell
build/server/cipple_server.exe 7001 replica-1.db --id 1 --peers '2@127.0.0.1:7002,3@127.0.0.1:7003'
build/server/cipple_server.exe 7002 replica-2.db --id 2 --peers '1@127.0.0.1:7001,3@127.0.0.1:7003'
build/server/cipple_server.exe 7003 replica-3.db --id 3 --peers '1@127.0.0.1:7001,2@127.0.0.1:7002'
```

服务默认监听环回地址。跨主机部署用 `--bind` 指定可达地址，并在 `--peers` 中使用各节点的 IP 和端口。当前部署契约要求受信网络与固定成员。默认
mesh 允许持有数据的接收节点继续传播，`--overlay star` 由源节点直连发送。单机仍可用 `cipple_server <port> <database>` 启动。

在单独的终端启动持久订阅。端点列表用于故障切换，同一个订阅进程重启后复用 `reader.db`。

```powershell
build/client/cipple_client.exe --state reader.db '7002,7003,7001' subscribe catalog feature
```

在另一个终端发布配置、查询完整基底的 UUID、提交增量并读取结果。

```powershell
build/client/cipple_client.exe 7001 put catalog feature 'mode=canary'
$base = build/client/cipple_client.exe 7001 get-base catalog feature
build/client/cipple_client.exe 7001 incremental catalog feature $base add_entry ';region=local'
build/client/cipple_client.exe 7001 get catalog feature
build/client/cipple_client.exe 7001 delete catalog feature
```

`get` 对存在的空字符串返回成功，对删除或不存在的键返回退出码 1。`add_entry` 拼接字面字符串，`remove_entry` 移除全部字面匹配。
`--selector hash` 按配置键选择首选端点，其余端点用于故障切换。持久订阅默认每 1 秒发心跳，并等待最多 1 秒，`--heartbeat-ms`
可调整。通过订阅进程的 Ctrl+C 发起同一会话的退订。写请求超时后，可使用原端点和原 `--request-id` 重试同一操作。

## 核对交付

构建后运行黑盒验收。脚本启动真实进程，注入回执丢失、网络分区、接收端拒绝较早的更新和发送端确认落盘失败。脚本读取 SQLite
文件核对持久化与确认进度。失败或阻塞的用例会使进程返回非零。

```powershell
py -3 tests/acceptance/run_acceptance.py --server build/server/cipple_server.exe --client build/client/cipple_client.exe
```

脚本输出临时目录中的 JSON、文本报告和 SHA256 校验值。运行库不在 PATH 时可传入 `--bin-path`。

- [行为、协议和保证](doc/behavior.md)
- [性能测量](bench/README.md)
- [验证与交付](doc/delivery.md)
