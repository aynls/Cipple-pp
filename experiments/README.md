# 复现 Java 基线与 C++ 对照

参考源码为 Ripple `468e9cb4296110467e3cfdafed412b3dbecfd52a`。脚本从该提交的 Git 归档构建 Java。运行环境需要 PowerShell 7、Git、JDK、Maven 和 Python 3。实验机使用 Temurin 25.0.3、Maven 3.9.9 和 Python 3.14.5。

## Java 功能冒烟

在 Cipple++ 仓库根目录运行以下命令。把 `PATH_TO_RIPPLE` 替换为 Java 参考仓库路径。两个仓库同目录相邻时，也可省略此参数。

```powershell
pwsh -File experiments/java/run-smoke.ps1 -RippleDir PATH_TO_RIPPLE
```

脚本启动独立 Java 服务端、客户端及使用原生客户端 API 的测试进程，验证全量写入、推送、读回、增量和删除。成功时输出 `SMOKE OK`。冒烟脚本通过轮询确认到达，延迟数字只用于功能验证。参见 [Java 冒烟样本](java/results/smoke-2026-11-20-manual/smoke-result.json)。

## 性能对照

构建 C++ Release 后，按[负载实验说明](load/README.md)运行。脚本写出实际发送跨度、每个订阅者的接收样本、统计值、进程采样、构建版本和错误信息。

[候选实测报告](evidence/README.md)汇总逐级放大的负载、提交后的 P99 和截止前覆盖。每行都可以进入原始配置和样本目录。
