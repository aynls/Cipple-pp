# 性能测量

从项目根目录构建测量工具。

```powershell
cmake --build build --target cipple_bench
build/bench/cipple_bench.exe codec --subs 32 --reps 1000 --value-bytes 256
build/bench/cipple_bench.exe fanout --subs 32 --ops 50 --warmup 5 --value-bytes 256
build/bench/cipple_bench.exe commit --ops 100
```

这三个命令分别测量编码、回环扇出和 SQLite 写入。将驱动连到一个正在监听的服务端，可以测量发送写入请求到收到分发帧的时间。

```powershell
build/bench/cipple_bench.exe drive --server 127.0.0.1:7001 --subs 8 --ops 30
py -3 -B bench/wire_driver.py --servers 127.0.0.1:7001 --app bench --key durable --subs 8 --ops 30 --resume-after 0 --persist-sqlite out/readers --out out/wire.json
```

`--resume-after` 指定持久订阅的起始游标。Python 驱动在解析分发帧后计时，启用 `--persist-sqlite`
时在订阅者事务提交后计时。每轮使用专用配置键，并在比较结果时记录相同的负载和计时边界。
