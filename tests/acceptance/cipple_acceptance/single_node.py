"""Exercise single-node behavior through the CLI and framed protocol."""

from __future__ import annotations

import time
import uuid
from pathlib import Path

from cipple_acceptance.protocol import (
    PUT_RESPONSE,
    SUBSCRIBE_RESPONSE,
    UNSUBSCRIBE_RESPONSE,
    Conn,
    decode_bool_response,
    decode_dispatch,
    decode_get,
    dispatch_response,
    get_request,
    put_request,
    subscribe_request,
    unsubscribe_request,
)
from cipple_acceptance.runtime import Child, cmdline, reserve_ports
from cipple_acceptance.sqlite_obs import format_db, journal_has

APP = b"acc"


def _case(name: str, status: str, detail: str, commands: list[str], observations: list[str]) -> dict:
    return {
        "name": name,
        "status": status,
        "detail": detail,
        "commands": commands,
        "observations": observations,
    }


def _start_server(binary: Path, db: Path, env: dict[str, str], work: Path) -> tuple[Child, int]:
    port = reserve_ports(1)[0]
    db.parent.mkdir(parents=True, exist_ok=True)
    args = [str(binary), str(port), str(db), "--bind", "127.0.0.1", "--id", "1"]
    child = Child(args, work, env)
    if not child.wait_for("Listening on", 8):
        detail = child.text()
        child.kill()
        raise RuntimeError(detail or "server produced no listening line")
    return child, port


def _cli(binary: Path, args: list[str], env: dict[str, str], work: Path, timeout: float = 12) -> tuple[int | None, str, str, str]:
    command = [str(binary), *args]
    child = Child(command, work, env)
    code = child.finish(timeout)
    return code, child.text("stdout"), child.text("stderr"), cmdline(command)


def _endpoint(port: int) -> str:
    return f"127.0.0.1:{port}"


def run_single_node(server_bin: Path, client_bin: Path, work: Path, env: dict[str, str]) -> list[dict]:
    cases: list[dict] = []
    try:
        server, port = _start_server(server_bin, work / "node.db", env, work)
    except Exception as error:
        return [_case("single_node_server", "blocked", "单节点服务没有进入监听。", [], [str(error)])]
    try:
        cases.append(_usage(server_bin, client_bin, env, work))
        cases.append(_cli_crud_and_increment(client_bin, port, env, work))
        cases.append(_cli_duplicate_and_empty(client_bin, port, env, work))
        cases.append(_protocol_bytes_and_subscription(port))
        cases.append(_subscriber_ack(port))
        cases.append(_masked_increment(client_bin, port, work / "node.db", env, work))
        cases.append(_benchmark(port))
    finally:
        server.kill()
    cases.append(_restart(server_bin, client_bin, work / "restart.db", env, work))
    return cases


def _usage(server_bin: Path, client_bin: Path, env: dict[str, str], work: Path) -> dict:
    server_code, _server_out, server_err, server_cmd = _cli(server_bin, [], env, work, 5)
    client_code, _client_out, client_err, client_cmd = _cli(client_bin, [], env, work, 5)
    return _case(
        "cli_usage",
        "pass",
        "记录当前二进制在无参数时公布的用法，后续断言都以此为准。",
        [server_cmd, client_cmd],
        [f"server exit={server_code}", server_err.strip(), f"client exit={client_code}", client_err.strip()],
    )


def _cli_crud_and_increment(client_bin: Path, port: int, env: dict[str, str], work: Path) -> dict:
    endpoint = _endpoint(port)
    base = uuid.UUID("00000000-0000-4000-8000-000000000011")
    removal = uuid.UUID("00000000-0000-4000-8000-000000000012")
    missing = uuid.UUID("00000000-0000-4000-8000-000000000013")
    commands: list[str] = []
    observations: list[str] = []

    def run(args: list[str]) -> tuple[int | None, str, str]:
        code, out, err, command = _cli(client_bin, ["--timeout-ms", "8000", *args], env, work)
        commands.append(command)
        observations.append(f"exit={code} stdout={out!r} stderr={err.strip()!r}")
        return code, out, err

    code, out, _err = run(["--request-id", str(base), endpoint, "put", "acc", "fold", "a-x-x-b"])
    if code != 0 or out != "OK\n":
        return _case("cli_crud_incremental", "fail", "全量写入没有返回 OK。", commands, observations)
    code, out, err = run([endpoint, "get", "acc", "fold"])
    if code != 0 or out != "a-x-x-b\n":
        return _case("cli_crud_incremental", "fail", "全量写入后读回的值不一致。", commands, observations)
    code, out, err = run([endpoint, "get-base", "acc", "fold"])
    if code != 0 or base.hex not in out.replace("-", ""):
        # Canonical UUID text contains the hex with hyphens.
        if str(base) not in out:
            return _case("cli_crud_incremental", "fail", "get-base 没有返回刚写入的 UUID。", commands, observations)
    code, out, _err = run(
        ["--request-id", str(removal), endpoint, "incremental", "acc", "fold", str(base), "remove_entry", "-x"]
    )
    if code != 0:
        return _case("cli_crud_incremental", "fail", "remove_entry 被拒绝。", commands, observations)
    code, out, _err = run([endpoint, "get", "acc", "fold"])
    if out != "a-b\n":
        return _case(
            "cli_crud_incremental",
            "fail",
            f"remove_entry 应把 a-x-x-b 折叠成 a-b，实际读回 {out!r}。",
            commands,
            observations,
        )
    code, _out, err = run(
        ["--request-id", str(missing), endpoint, "incremental", "acc", "fold", str(uuid.uuid4()), "add_entry", "z"]
    )
    if code == 0 or "Request rejected" not in err:
        return _case("cli_crud_incremental", "fail", "缺少基线的增量没有被明确拒绝。", commands, observations)
    observations.append("缺少基线的增量被拒绝，已有折叠结果保持 a-b。")
    code, out, _err = run(["--request-id", str(uuid.uuid4()), endpoint, "delete", "acc", "fold"])
    if code != 0:
        return _case("cli_crud_incremental", "fail", "删除没有返回 OK。", commands, observations)
    code, out, err = run([endpoint, "get", "acc", "fold"])
    if code == 0 or "Configuration not found" not in err:
        return _case("cli_crud_incremental", "fail", "删除后的键仍然可见。", commands, observations)
    return _case(
        "cli_crud_incremental",
        "pass",
        "CLI 完成了写入、读回、get-base、remove_entry 全量折叠、缺基线拒绝和删除。",
        commands,
        observations,
    )


def _cli_duplicate_and_empty(client_bin: Path, port: int, env: dict[str, str], work: Path) -> dict:
    endpoint = _endpoint(port)
    request_id = uuid.UUID("00000000-0000-4000-8000-000000000021")
    commands: list[str] = []
    observations: list[str] = []

    def run(args: list[str]) -> tuple[int | None, str, str]:
        code, out, err, command = _cli(client_bin, ["--timeout-ms", "8000", *args], env, work)
        commands.append(command)
        observations.append(f"exit={code} stdout={out!r} stderr={err.strip()!r}")
        return code, out, err

    code, out, _err = run(["--request-id", str(request_id), endpoint, "put", "acc", "dup", "initial"])
    if code != 0:
        return _case("cli_idempotent_put", "fail", "第一次写入失败。", commands, observations)
    code, _out, err = run(["--request-id", str(request_id), endpoint, "put", "acc", "dup", "conflict"])
    if code == 0:
        return _case("cli_idempotent_put", "fail", "同一 UUID 改了负载仍被接受。", commands, observations)
    observations.append(f"不同负载被拒绝: {err.strip()}")
    code, out, _err = run(["--request-id", str(request_id), endpoint, "put", "acc", "dup", "initial"])
    if code != 0:
        return _case("cli_idempotent_put", "fail", "相同负载的重试没有被当成已完成。", commands, observations)
    code, out, _err = run([endpoint, "get", "acc", "dup"])
    if out != "initial\n":
        return _case("cli_idempotent_put", "fail", f"重试后期望 initial，实际得到 {out!r}。", commands, observations)
    code, out, _err = run(["--request-id", str(uuid.uuid4()), endpoint, "put", "acc", "empty", ""])
    if code != 0:
        return _case("cli_idempotent_put", "fail", "空字符串写入失败。", commands, observations)
    code, out, err = run([endpoint, "get", "acc", "empty"])
    if code != 0 or out != "\n":
        return _case("cli_idempotent_put", "fail", f"空字符串读回是 {out!r} / {err!r}。", commands, observations)
    return _case(
        "cli_idempotent_put",
        "pass",
        "同一 UUID 只有负载一致时才能重试成功。读取可以区分空字符串和缺失。",
        commands,
        observations,
    )


def _protocol_bytes_and_subscription(port: int) -> dict:
    observations: list[str] = []
    writer = Conn("127.0.0.1", port)
    reader = Conn("127.0.0.1", port)
    try:
        nul_id = uuid.uuid4()
        body = writer.request(put_request(nul_id, APP, b"nul", b"a\x00b"))
        if not decode_bool_response(body, PUT_RESPONSE):
            return _case("protocol_subscription", "fail", "含 NUL 的写入被拒绝。", [], observations)
        reply = decode_get(writer.request(get_request(uuid.uuid4(), APP, b"nul")))
        observations.append(f"nul visible={reply.visible!r}")
        if reply.visible != b"a\x00b":
            return _case("protocol_subscription", "fail", "含 NUL 的值没有按长度前缀原样读回。", [], observations)

        subscribed = decode_bool_response(
            reader.request(subscribe_request(uuid.uuid4(), APP, b"live")), SUBSCRIBE_RESPONSE
        )
        observations.append(f"subscribe={subscribed}")
        if not subscribed:
            return _case("protocol_subscription", "fail", "空回调地址的会话内订阅被拒绝。", [], observations)
        writer.request(put_request(uuid.uuid4(), APP, b"live", b"one"))
        dispatch = decode_dispatch(reader.read_frame(3))
        observations.append(f"dispatch value={dispatch.entity.value!r} uuid={dispatch.entity.uuid}")
        if dispatch.entity.value != b"one":
            return _case("protocol_subscription", "fail", "订阅者没有收到本次写入。", [], observations)
        writer.request(put_request(uuid.uuid4(), APP, b"other", b"nope"))
        try:
            stray = reader.read_frame(0.4)
        except (TimeoutError, OSError):
            stray = None
        if stray is not None:
            return _case("protocol_subscription", "fail", "其他键的写入推进了这条订阅。", [], [str(stray[:40])])
        if not decode_bool_response(
            reader.request(unsubscribe_request(uuid.uuid4(), APP, b"live")), UNSUBSCRIBE_RESPONSE
        ):
            return _case("protocol_subscription", "fail", "退订被拒绝。", [], observations)
        writer.request(put_request(uuid.uuid4(), APP, b"live", b"two"))
        try:
            after = reader.read_frame(0.4)
        except (TimeoutError, OSError):
            after = None
        if after is not None:
            return _case("protocol_subscription", "fail", "退订之后仍然收到推送。", [], observations)
        return _case(
            "protocol_subscription",
            "pass",
            "长度前缀保留 NUL；会话内订阅只接收目标键，退订后停止推送。",
            [],
            observations,
        )
    finally:
        writer.close()
        reader.close()


def _subscriber_ack(port: int) -> dict:
    observations: list[str] = []
    writer = Conn("127.0.0.1", port, 5)
    reader = Conn("127.0.0.1", port, 5)
    request_id = uuid.uuid4()
    try:
        ok = decode_bool_response(
            reader.request(subscribe_request(uuid.uuid4(), APP, b"ack", resume_after=0)), SUBSCRIBE_RESPONSE
        )
        observations.append(f"subscribe={ok}")
        if not ok:
            return _case("subscriber_ack", "fail", "带游标 0 的订阅被拒绝。", [], observations)
        if not decode_bool_response(writer.request(put_request(request_id, APP, b"ack", b"durable")), PUT_RESPONSE):
            return _case("subscriber_ack", "fail", "写入失败。", [], observations)
        first = decode_dispatch(reader.read_frame(3))
        observations.append(f"first sequence={first.sequence} uuid={first.entity.uuid}")
        if first.entity.uuid != request_id or first.sequence <= 0:
            return _case("subscriber_ack", "fail", "持久订阅没有带正的日志序号。", [], observations)
        closed = False
        try:
            extra = reader.read_frame(4)
            observations.append(f"unexpected frame type={extra[0]}")
        except TimeoutError as error:
            observations.append(f"session stayed open after withheld ack: {error}")
        except (EOFError, OSError) as error:
            closed = True
            observations.append(f"withheld ack ended the session: {type(error).__name__}: {error}")
        reader.close()
        if not closed:
            return _case("subscriber_ack", "fail", "不回执时服务端没有在超时内断开。", [], observations)
        reader = Conn("127.0.0.1", port, 5)
        decode_bool_response(
            reader.request(subscribe_request(uuid.uuid4(), APP, b"ack", resume_after=0)), SUBSCRIBE_RESPONSE
        )
        second = decode_dispatch(reader.read_frame(3))
        observations.append(f"replay uuid={second.entity.uuid} sequence={second.sequence}")
        if second.entity.uuid != request_id:
            return _case("subscriber_ack", "fail", "重连后没有重放未确认的那条消息。", [], observations)
        reader.send(dispatch_response(second.uuid, True))
        try:
            third = reader.read_frame(0.8)
        except (TimeoutError, OSError):
            third = None
        if third is not None:
            return _case("subscriber_ack", "fail", "确认之后同一条消息又被推了一次。", [], observations)
        return _case(
            "subscriber_ack",
            "pass",
            "不回执会在约 2 秒后断开会话；用原游标重连会重放，回执后不再重复。",
            [],
            observations,
        )
    finally:
        writer.close()
        reader.close()


def _masked_increment(client_bin: Path, port: int, db: Path, env: dict[str, str], work: Path) -> dict:
    endpoint = _endpoint(port)
    state = work / "subscriber.db"
    base = uuid.UUID("00000000-0000-4000-8000-000000000031")
    inc = uuid.UUID("00000000-0000-4000-8000-000000000032")
    deletion = uuid.UUID("00000000-0000-4000-8000-000000000033")
    commands: list[str] = []
    observations: list[str] = []
    args = [
        str(client_bin),
        "--state",
        str(state),
        "--timeout-ms",
        "15000",
        endpoint,
        "subscribe",
        "acc",
        "mask",
    ]
    subscriber = Child(args, work, env)
    commands.append(cmdline(args))
    try:
        if not subscriber.wait_for("Subscribed", 8, "stdout"):
            observations.append(subscriber.text())
            return _case("masked_increment_catchup", "fail", "订阅者没有进入 Subscribed。", commands, observations)
        code, out, err, command = _cli(
            client_bin, ["--request-id", str(base), "--timeout-ms", "8000", endpoint, "put", "acc", "mask", "base"], env, work
        )
        commands.append(command)
        observations.append(f"put exit={code} stdout={out!r} stderr={err.strip()!r}")
        if code != 0 or not subscriber.wait_for("base", 5, "stdout"):
            observations.append(subscriber.text())
            return _case("masked_increment_catchup", "fail", "在线订阅者没有看到全量值。", commands, observations)
    finally:
        subscriber.kill()

    def run(args: list[str]) -> tuple[int | None, str, str]:
        code, out, err, command = _cli(client_bin, ["--timeout-ms", "8000", *args], env, work)
        commands.append(command)
        observations.append(f"exit={code} stdout={out!r} stderr={err.strip()!r}")
        return code, out, err

    code, _out, err = run(
        ["--request-id", str(inc), endpoint, "incremental", "acc", "mask", str(base), "add_entry", "+inc"]
    )
    if code != 0:
        return _case("masked_increment_catchup", "fail", f"增量失败: {err.strip()}", commands, observations)
    code, _out, err = run(["--request-id", str(deletion), endpoint, "delete", "acc", "mask"])
    if code != 0:
        return _case("masked_increment_catchup", "fail", f"删除失败: {err.strip()}", commands, observations)
    code, out, err = run([endpoint, "get", "acc", "mask"])
    observations.append(format_db(db))
    if "Configuration not found" not in err:
        return _case("masked_increment_catchup", "fail", f"删除没有遮蔽可见值，get 返回 {out!r} {err!r}。", commands, observations)
    if not journal_has(db, inc):
        return _case("masked_increment_catchup", "fail", "删除之后 delivery_log 里没有增量。", commands, observations)

    resumed = Child(
        [str(client_bin), "--state", str(state), "--timeout-ms", "15000", endpoint, "subscribe", "acc", "mask"],
        work,
        env,
    )
    commands.append(cmdline(resumed.args))
    try:
        saw_inc = resumed.wait_for("base+inc", 8, "stdout") or resumed.wait_for(str(inc), 1, "stderr")
        saw_delete = resumed.wait_for("<deleted>", 8, "stdout")
        observations.append("stdout after resume:\n" + resumed.text("stdout"))
        observations.append("stderr after resume:\n" + resumed.text("stderr"))
    finally:
        resumed.kill()
    if not saw_inc or not saw_delete:
        return _case(
            "masked_increment_catchup",
            "fail",
            "断线后的游标补发没有同时给出增量和随后的删除。",
            commands,
            observations,
        )
    return _case(
        "masked_increment_catchup",
        "pass",
        "可见值已被删除遮蔽，但日志和重连补发都保留了那条增量，并且最终补到删除。",
        commands,
        observations,
    )


def _benchmark(port: int) -> dict:
    samples: list[float] = []
    conn = Conn("127.0.0.1", port, 5)
    try:
        for index in range(30):
            started = time.perf_counter()
            body = conn.request(put_request(uuid.uuid4(), APP, b"bench", f"v{index}".encode()))
            if not decode_bool_response(body, PUT_RESPONSE):
                return _case("put_rtt", "fail", f"第 {index} 次写入被拒绝。", [], [])
            samples.append((time.perf_counter() - started) * 1000)
    finally:
        conn.close()
    ordered = sorted(samples)
    summary = (
        f"n=30 min={ordered[0]:.3f}ms p50={ordered[len(ordered)//2]:.3f}ms "
        f"max={ordered[-1]:.3f}ms"
    )
    return _case(
        "put_rtt",
        "pass",
        "单连接顺序 put 的往返时间。",
        [],
        [summary],
    )


def _restart(server_bin: Path, client_bin: Path, db: Path, env: dict[str, str], work: Path) -> dict:
    commands: list[str] = []
    observations: list[str] = []
    try:
        server, port = _start_server(server_bin, db, env, work)
    except Exception as error:
        return _case("durable_restart", "blocked", "重启用例的服务没有监听。", [], [str(error)])
    request_id = uuid.UUID("00000000-0000-4000-8000-000000000041")
    try:
        code, out, err, command = _cli(
            client_bin,
            ["--request-id", str(request_id), "--timeout-ms", "8000", _endpoint(port), "put", "acc", "keep", "kept"],
            env,
            work,
        )
        commands.append(command)
        observations.append(f"put exit={code} stdout={out!r} stderr={err.strip()!r}")
        if code != 0:
            return _case("durable_restart", "fail", "崩溃前的写入没有成功。", commands, observations)
    finally:
        server.kill()
    observations.append("server killed after OK; database file retained")
    try:
        server, port = _start_server(server_bin, db, env, work)
    except Exception as error:
        observations.append(str(error))
        return _case("durable_restart", "fail", "保留数据库后的重启没有进入监听。", commands, observations)
    try:
        code, out, err, command = _cli(
            client_bin, ["--timeout-ms", "8000", _endpoint(port), "get", "acc", "keep"], env, work
        )
        commands.append(command)
        observations.append(f"get exit={code} stdout={out!r} stderr={err.strip()!r}")
        observations.append(format_db(db))
        if out != "kept\n":
            return _case("durable_restart", "fail", "重启后没有读回崩溃前已确认的值。", commands, observations)
        return _case(
            "durable_restart",
            "pass",
            "写入返回成功之后进程被终止，原数据库重新打开仍能读回该值。",
            commands,
            observations,
        )
    finally:
        server.kill()
