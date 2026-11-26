# 验证与交付

## Windows 构建

在目标架构为 x64 的 Visual Studio Developer PowerShell 中构建。需要 MSVC C++ 工具集、Windows SDK、CMake 3.28 或更新版本、Ninja 和 Git。独立进程验收和示例需要 Windows Python 3 启动器 `py`。

使用 vcpkg `2026.10.16` 的 registry，可在该标签的检出目录运行 `bootstrap-vcpkg.bat` 获取配套的 `2026-09-26` 工具。将当前开发者终端的 `VCPKG_ROOT` 设为此目录。Visual Studio 自带的旧版 vcpkg 可能无法读取该基线的工具清单。 [vcpkg registry 发行版](https://github.com/microsoft/vcpkg/releases/tag/2026.10.16) · [vcpkg 工具发行版](https://github.com/microsoft/vcpkg-tool/releases/tag/2026-09-26)

如果 vcpkg 报告无法访问 `Microsoft\WindowsApps` 下的 `pwsh.exe` 别名，可在当前开发者终端使用 PowerShell 的实际安装目录。

```powershell
$windowsAppsAliases = Join-Path $env:LOCALAPPDATA 'Microsoft\WindowsApps'
$remainingPaths = $env:PATH.Split(';', [StringSplitOptions]::RemoveEmptyEntries) | Where-Object {
  -not $_.Trim('"').StartsWith($windowsAppsAliases, [StringComparison]::OrdinalIgnoreCase)
}
$env:PATH = (@($PSHOME) + @($remainingPaths)) -join ';'
```

在项目根目录执行以下命令。首次配置时，vcpkg 从 `vcpkg.json` 指定的基线构建静态库。

```powershell
cmake --preset windows-release
cmake --build --preset windows-release --parallel 6
ctest --preset windows-release
py -3 -B tests/acceptance/run_acceptance.py --server build/windows-release/server/cipple_server.exe --client build/windows-release/client/cipple_client.exe
```

调试构建可使用 `windows-debug` 预设，产物位于 `build/windows-debug`。

CTest 覆盖帧编解码、会话、SQLite 事务、订阅存储和单节点请求。黑盒脚本启动独立进程，验证以下行为。

- 请求 UUID 去重、增量恢复、删除和读取结果。
- 实时订阅、持久回放、订阅回执和客户端换节点。
- 多数派提交、停止 Leader 后换主以及孤立节点拒绝写入和查询。
- 离线副本安装快照、追赶尾部日志、进程重启以及过旧订阅游标处理。

结果记录在控制台显示的报告目录。文本报告列出命令和观测，JSON 便于后续处理。SHA256 文件记录被测二进制与报告的校验值。

## 三节点示例

在当前开发者 PowerShell 中运行 `./scripts/demo.ps1` 可构建并验证三节点的持久订阅。已有 Release 构建时，可以传入 `-SkipBuild` 直接运行示例。测量提交与分发开销时使用性能测量工具。 [性能测量](../bench/README.md)

下面的三个命令各在一个终端运行。应用端口是 7001 至 7003，节点间 Raft 端口是 7101 至 7103。

```powershell
./build/windows-release/server/cipple_server.exe 7001 replica-1.db --id 1 --raft-port 7101 --peers '2@127.0.0.1:7102,3@127.0.0.1:7103'
./build/windows-release/server/cipple_server.exe 7002 replica-2.db --id 2 --raft-port 7102 --peers '1@127.0.0.1:7101,3@127.0.0.1:7103'
./build/windows-release/server/cipple_server.exe 7003 replica-3.db --id 3 --raft-port 7103 --peers '1@127.0.0.1:7101,2@127.0.0.1:7102'
```

客户端命令使用应用端口。发布和查询可在多个端点上自动寻找可提交请求的节点。

```powershell
./build/windows-release/client/cipple_client.exe '127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003' put demo feature 'mode=canary'
```

## 数据文件

每个节点保留应用数据库及同目录的 `.raft` 日志库和快照文件，连同 SQLite 的 WAL 一起管理。持久化库绑定成员 ID 和 Raft 地址。单节点省略 `--peers` 和 `--raft-port` 即可运行。多机部署通过 `--raft-host` 声明本节点的 IPv4 地址。

旧版单节点数据库可以沿用，启动时从旧版本和游标之后分配新的位置。旧版多主集群应先用旧程序确定需保留的配置，再把全量值发布到新集群。新版集群拒绝就地打开带旧配置的数据库，避免不同副本从不同状态启动。旧客户端的端点游标从 0 重新回放，保留已接收事件的 UUID 去重。遇到历史已裁剪时，可根据客户端给出的最小游标显式选择 `--resume-after`。
