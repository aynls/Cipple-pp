# 验证与交付

在完成[环境配置](../README.md#构建并运行演示)后，从项目根目录构建并运行测试。

```powershell
cmake --build build --parallel 6
ctest --test-dir build --output-on-failure
py -3 -B tests/acceptance/run_acceptance.py --server build/server/cipple_server.exe --client build/client/cipple_client.exe
```

CTest 覆盖帧编解码、会话、SQLite 事务、确认队列、订阅存储和单节点请求。黑盒脚本启动独立进程，验证以下行为。

- 请求 UUID 去重、增量恢复、删除和读取结果。
- 实时订阅、持久回放、订阅回执和客户端换节点。
- 分区恢复、丢失复制回执、源节点退出后的 mesh 转发。
- 复制积压中较早的配置被拒绝时，后续配置继续送达。
- 发送端确认落盘失败时，本地重试并保持同一实体的发送次数。

结果记录在控制台显示的报告目录。文本报告列出命令和观测，JSON 便于后续处理。SHA256 文件记录被测二进制与报告的校验值。

运行 `pwsh -File scripts/demo.ps1 -SkipBuild` 可以从 CLI 验证三节点的持久订阅。测量本地提交与分发开销时使用[性能测量工具](../bench/README.md)。
