# 手动小验证记录（2026-11-20，单机 smoke，全绿）

本目录 = `java/results/smoke-2026-11-20-manual/`，对应 `run-smoke.ps1` 的手动首跑。
端口 13101（server API）/14101（server UI）/15101（client API）/16101（client UI），均为隔离测试端口。

## 构建（Java 基线，隔离归档，`BUILD SUCCESS`）

```powershell
git -C 'D:\UCAS\OOPLab\Ripple' archive --format=tar HEAD | tar -x -C 'C:\Users\21423\AppData\Local\Temp\opencode\ripple-baseline'
& 'C:\Users\21423\AppData\Local\Temp\opencode\apache-maven-3.9.9\bin\mvn.cmd' -f 'C:\Users\21423\AppData\Local\Temp\opencode\ripple-baseline\pom.xml' install -DskipTests
```

- 基线：`468e9cb`（`git rev-parse HEAD` 实测），工作树未动（`.gitignore` 的 `M` 与 `scripts/`、`*.md` untracked 均保持原样）。
- 工具链：Maven 3.9.9（`archive.apache.org` 断点续传），JDK `25.0.3+9-LTS (Temurin HotSpot)`。
- Reactor：`ripple / ripple-common / ripple-server / ripple-client ... SUCCESS [01:23 min]`。
- 产物：`ripple-server/target/ripple-server.jar + ripple-server-publish.tar.gz`，
  `ripple-client/target/ripple-client.jar + ripple-client-publish.tar.gz`（`lib/` 含 netty/sqlite/jetty/jackson/oshi）。

## 运行（3 个 OS 进程，真实 TCP）

```powershell
# server（standalone，多进程之一）
java -Did=1 -Daddress="127.0.0.1" -DapiPort=13101 -DuiPort=14101 -DstorageLocation="...server1\node-1.db" -Dnodes="1:127.0.0.1:13101" -Dprotocol=star -Dorg.slf4j.simpleLogger.defaultLogLevel=INFO -jar ...server1\ripple-server.jar
# client（standalone，多进程之二；证明 server→client 推送通路 + SYSTEM_INFO 回路）
java -Daddress="127.0.0.1" -DapiPort=15101 -DuiPort=16101 -DstorageLocation="...client1\client-1.db" -Dnodes="1:127.0.0.1:13101" -DnodeSelector=hashing ... -jar ...client1\ripple-client.jar
# driver（多进程之三；embedded pub/sub 各 1，直连 server 集群做可度量 workload）
java -cp "<driver>;<server1\lib\*.jar>;<classes>" -Dorg.slf4j.simpleLogger.defaultLogLevel=WARN Driver "1:127.0.0.1:13101" smokeapp smokekey "...\driverdb" "...\smoke-result.json"
```

- server 启动行：`[Main] Ripple Server started. The API port is 13101. The UI port is 14101.`
- server 自连接 + 心跳：`Connecting to 127.0.0.1:13101` → `Send HEARTBEAT` → `Set liveness ... to true`（单节点 star 仍走自连）。
- client：`Ripple Client started ... 15101/16101`，`doConnect: Completed`，`Send SYSTEM_INFO` → `CPU Usage of 1 is 0.187...`。
- driver：`Driver.java`（见 `experiments/java/Driver.java`），subscribe 后 sleep 2s 等异步回路，
  以 subscriber 存储 `findMessages(app,key)` 计数 +1 为到达边界（200ms 轮询，15s 超时）。

## 结果原文（`smoke-result.json`）

```json
{
  "baseline": "ripple-468e9cb",
  "ok": true,
  "steps": ["clients_started", "subscribed", "dispatch_observed", "get_merged_ok", "incremental_observed", "delete_observed", "delete_merged_ok"],
  "put_value": "smoke-687e07a6-623e-496d-8be2-ad44f3a3c998",
  "merged_after_put": "smoke-687e07a6-623e-496d-8be2-ad44f3a3c998",
  "merged_after_inc": "smoke-687e07a6-623e-496d-8be2-ad44f3a3c998-inc",
  "merged_after_delete": "null",
  "publish_to_subscriber_ms": 225,
  "get_roundtrip_ms": 2021,
  "incremental_to_subscriber_ms": 221,
  "delete_to_subscriber_ms": 222,
  "sub_msgs_before": 0, "sub_msgs_after_put": 1, "sub_msgs_after_inc": 2, "sub_msgs_after_delete": 3
}
```

`get_roundtrip_ms` 含脚本内 2s settle，不可与 publish→subscriber 直接对比；其余三项为同口径轮询延迟。

## server 分发证据（`server-dispatch-excerpt.log`）

- `SubscribeRequestHandler ... Receive SUBSCRIBE ... smokekey, callback 127.0.0.1:51064` → `subscribe() called` → `Send SUBSCRIBE response. Success = true`
- `PutRequestHandler ... Receive PUT ... value = smoke-687e...`
- `EqualDivisionClientDispatcher Node-1: Sync update message (...) to client 127.0.0.1:51064`
- `... Sync incremental_update message (...) to client 127.0.0.1:51064`
- `... Sync delete message (...) to client 127.0.0.1:51064`

注意：日志类名为 `DefaultClientDispatcher`，但消息体为 `EqualDivisionClientDispatcher`
（`Node` 默认装配的是后者，`Node.java:306`），报告时以行为为准。

## 与任务书 §6.2 的对应（行为确认）

- client→server 写：`PutRequestHandler → Node.put → propagateMessage → 先落盘后异步 Sync`，PUT response 在落盘后返回。
- server→client：订阅走 `Node.subscribe` + `EqualDivisionClientDispatcher.notifyClients → Api.sync(channel, message)`，
  即“SYNC 复用为 server→client 推送”，与 `DispatchRequest`（client 间 P2P 透传用）不同链路。
- 幂等：`propagateMessage` 先查 `messageService.exist(uuid)`，`DUPLICATED` 直接回；client `SyncRequestHandler.applyMessage` 同理。
- 健康检查现状：`HealthManager.checkHealth` 只对 `alive==false` 的节点发心跳，活节点永不复查；
  `Tracker.retry` 由 `Worker` 每 10s 驱动（`Worker.java:37-50`），`Node.doSyncWithServer` 写 socket 即 `recordAck`。
