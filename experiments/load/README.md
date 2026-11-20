# 运行同负载对照

先按[仓库说明](../../README.md)构建 C++ Release，并准备固定版本的 Java 参考仓库。每次运行在同一主机顺序启动两个实现。Java 阶段的进程退出后，C++ 阶段开始。

```powershell
pwsh -File experiments/load/run-comparison.ps1 -Servers 3 -Subscribers 6 -Warmup 8 -Ops 100 -GapMs 200 -JavaEventLoopThreads 2 -JavaLogLevel ERROR -MeasurePersist
```

默认从 `build/server/cipple_server.exe` 启动候选。可用 `-CippleServerExe` 指定 Release 二进制，用 `-CippleCodeRevision` 记录构建所用提交。脚本还记录二进制 SHA256。Java 源检出可通过 `-RippleDir` 指定。可复用前一次日志中 `java_build_source` 指向的构建目录，传给 `-JavaBuildReuse`。脚本检查目录里的 `.reuse-pin.txt` 和产物。安装在 PATH 外的工具可用 `-MavenCmd`、`-JavaExe`、`-JavacExe`、`-PythonExe` 和 `-MsysBin` 指定。

## 工作负载和观测

| 项 | 设置 |
| --- | --- |
| 复制 | 默认两版星形，全部节点为目标。可用 `-CppOverlay mesh` 测 C++ 转发开销 |
| 放置 | 发布者连接节点 1，订阅者按节点索引轮转 |
| 模式 | 单热键的全量更新，默认 256 字节。预热后按单调时钟计划间隔连续发包 |
| 延迟 | 从发布端发包至订阅者观察到对应序号。由同一驱动进程的单调时钟计时 |
| 有效吞吐 | 被所有预期订阅者观察到的发布数，除以从第一条发布到统计截止的时间 |
| 资源 | 按服务端进程采样 CPU 总时间和工作集。用时间戳核对测量窗口 |
| 稳定 | 所有配对完成、运行无错误、有效吞吐达到计划速率的 90%，尾部排空耗时至多 1 秒 |

Java 驱动使用原生 `RippleClient`，在订阅者 SQLite 插入成功后记录。C++ 的 `-MeasurePersist` 让 Python 协议驱动使用持久订阅，在每个订阅者自己的 SQLite WAL/FULL 事务提交后记录和回执。此模式用于两个提交边界的比较。C++ live 另有即时收包样本，`-ProbeDurable` 可采样只有游标的线路开销。原生 C++ 客户端单独通过独立集成测试验证。

默认将每轮结果写入 `experiments/load/results/<run>/`，可用 `-OutputRoot` 指定其他目录。保留 `trace.json`、`comparison.json`、逐事件 `*-summary.jsonl` 和 `cpu-rss.csv`。未按截止时间送达的配对保留在样本中。端口可用 `-JavaApiBase`、`-JavaUiBase` 和 `-CppBase` 调整。

复跑千订阅实验时使用下面的参数。Java 或 C++ 的任一阶段遇到超时或覆盖不全时，编排返回非零，并保留能够生成的各阶段结果。

```powershell
pwsh -File experiments/load/run-comparison.ps1 -Servers 10 -Subscribers 1000 -Warmup 2 -Ops 22 -GapMs 1000 -JavaEventLoopThreads 2 -JavaLogLevel ERROR -MeasurePersist
```
