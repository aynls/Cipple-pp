"""Three-node fault scenario.

The candidate dials peer addresses, so those addresses are proxies. C starts
partitioned. A commits an incremental, B persists it, and the proxy drops the
one SyncResponse that would confirm B. A is then killed with
its database kept. After restart the retry may be confirmed once; the
incremental must still exist after a later delete masks the visible value.
"""

from __future__ import annotations

import time
import traceback
import uuid
from pathlib import Path

from cipple_acceptance.proxy import DropPlan, Proxy
from cipple_acceptance.runtime import Child, cmdline, reserve_ports
from cipple_acceptance.sqlite_obs import ack_bit, format_db, journal_has, message_count, message_hlc

BASE = uuid.UUID("00000000-0000-4000-8000-0000000000a1")
INC = uuid.UUID("00000000-0000-4000-8000-0000000000a2")
DELETE = uuid.UUID("00000000-0000-4000-8000-0000000000a3")
SKEW = uuid.UUID("00000000-0000-4000-8000-0000000000a4")


def _case(name: str, status: str, detail: str, commands: list[str], observations: list[str]) -> dict:
    return {
        "name": name,
        "status": status,
        "detail": detail,
        "commands": list(commands),
        "observations": list(observations),
    }


def run_combined(server_bin: Path, client_bin: Path, work: Path, env: dict[str, str]) -> list[dict]:
    work.mkdir(parents=True, exist_ok=True)
    ports = reserve_ports(10)
    port_a, port_b, port_c, p_ab, p_ac, p_ba, p_bc, p_ca, p_cb, dead = ports
    db_a, db_b, db_c = work / "a.db", work / "b.db", work / "c.db"
    plan = DropPlan(INC)
    proxies = [
        Proxy("ab", p_ab, port_b, plan),
        Proxy("ba", p_ba, port_a, plan),
        Proxy("ac", p_ac, port_c),
        Proxy("ca", p_ca, port_a),
        Proxy("bc", p_bc, port_c),
        Proxy("cb", p_cb, port_b),
    ]
    for proxy in proxies:
        if proxy.name in {"ac", "ca", "bc", "cb"}:
            proxy.set_refusing(True)
    children: list[Child] = []
    commands: list[str] = []
    observations = [
        f"ports A={port_a} B={port_b} C={port_c} dead={dead}",
        f"proxies ab={p_ab} ba={p_ba} ac={p_ac} ca={p_ca} bc={p_bc} cb={p_cb}",
        "C 方向的四条代理从启动起拒绝连接；A-B 保持连通，直到增量的持久化确认被丢弃。",
    ]

    def cleanup() -> None:
        for child in children:
            child.kill()
        for proxy in proxies:
            proxy.close()

    def launch(node_id: int, port: int, db: Path, peers: str, offset: int) -> Child:
        args = [
            str(server_bin),
            str(port),
            str(db),
            "--bind",
            "127.0.0.1",
            "--id",
            str(node_id),
            "--peers",
            peers,
            "--overlay",
            "star",
            "--clock-offset-ms",
            str(offset),
        ]
        child = Child(args, work, env)
        children.append(child)
        commands.append(cmdline(args))
        if not child.wait_for("Listening on", 8):
            detail = child.text()
            child.kill()
            raise RuntimeError(f"node {node_id} 没有监听\n{detail}")
        return child

    def cli(args: list[str], timeout: float = 12) -> tuple[int | None, str, str]:
        command = [str(client_bin), "--timeout-ms", "8000", *args]
        child = Child(command, work, env)
        children.append(child)
        commands.append(cmdline(command))
        code = child.finish(timeout)
        return code, child.text("stdout"), child.text("stderr")

    def visible(port: int) -> str | None:
        code, out, err = cli([f"127.0.0.1:{port}", "get", "acc", "cfg"])
        if code == 0:
            return out.rstrip("\r\n")
        if code == 1 and "Configuration not found" in err:
            return None
        raise RuntimeError(f"get 127.0.0.1:{port} exit={code} stdout={out!r} stderr={err.strip()!r}")

    def wait_visible(port: int, expected: str | None, timeout: float) -> str | None:
        deadline = time.monotonic() + timeout
        last: str | None = "unread"
        while time.monotonic() < deadline:
            last = visible(port)
            if last == expected:
                return last
            time.sleep(0.25)
        observations.append(f"wait 127.0.0.1:{port} wanted {expected!r} last {last!r}")
        return last

    try:
        peers_a = f"2@127.0.0.1:{p_ab},3@127.0.0.1:{p_ac}"
        peers_b = f"1@127.0.0.1:{p_ba},3@127.0.0.1:{p_bc}"
        peers_c = f"1@127.0.0.1:{p_ca},2@127.0.0.1:{p_cb}"
        server_a = launch(1, port_a, db_a, peers_a, 0)
        server_b = launch(2, port_b, db_b, peers_b, 0)
        server_c = launch(3, port_c, db_c, peers_c, -86_400_000)

        code, out, err = cli(["--request-id", str(SKEW), f"127.0.0.1:{port_c}", "put", "acc", "cfg", "skew"])
        observations.append(f"skew put exit={code} stdout={out!r} stderr={err.strip()!r}")
        if code != 0:
            return [_case("cluster_fault", "fail", "分区中的 C 没能在本地提交偏移写入。", commands, observations)]

        code, out, err = cli(["--request-id", str(BASE), f"127.0.0.1:{port_a}", "put", "acc", "cfg", "base"])
        observations.append(f"base put exit={code} stdout={out!r} stderr={err.strip()!r}")
        if code != 0 or wait_visible(port_b, "base", 15) != "base":
            observations.extend(_dumps(db_a, db_b, db_c))
            return [_case("cluster_fault", "fail", "基线没有在返回成功后出现在 B 上。", commands, observations)]
        if wait_visible(port_a, "base", 5) != "base":
            return [_case("cluster_fault", "fail", "源节点自己读不到刚确认的基线。", commands, observations)]

        failover = _failover(cli, dead, port_b, commands, observations)

        state = work / "subscriber.db"
        first = _subscribe(client_bin, state, port_b, work, env, children, commands)
        if not first.wait_for("Subscribed", 8, "stdout") or not first.wait_for("\nbase\n", 8,
                                                                               "both") and "base" not in first.text(
            "stdout"):
            observations.append(first.text())
            first.kill()
            return [failover, _case("cluster_fault", "fail", "B 上的订阅者没有收到基线。", commands, observations)]
        observations.append("subscriber saw base, then went offline with its cursor database kept")
        first.kill()

        code, out, err = cli(
            ["--request-id", str(INC), f"127.0.0.1:{port_a}", "incremental", "acc", "cfg", str(BASE), "add_entry",
             "+inc"]
        )
        observations.append(f"incremental put exit={code} stdout={out!r} stderr={err.strip()!r}")
        if code != 0:
            return [failover, _case("cluster_fault", "fail", "增量写入没有在源节点成功。", commands, observations)]

        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and not (plan.dropped >= 1 and visible(port_b) == "base+inc"):
            time.sleep(0.25)
        bit = ack_bit(db_a, INC, 2)
        observations.append(f"dropped_confirmations={plan.dropped} refusing={plan.refusing} ack_b={bit}")
        observations.append(format_db(db_b))
        if plan.dropped != 1 or visible(port_b) != "base+inc" or bit != 0:
            observations.extend(_frame_notes(plan))
            observations.append(format_db(db_a))
            return [
                failover,
                _case(
                    "cluster_fault",
                    "fail",
                    "没有形成“B 已持久化增量、确认帧被丢弃恰好一次、A 仍把 B 记为未确认”的状态。",
                    commands,
                    observations,
                ),
            ]

        server_a.kill()
        observations.append("killed A after the dropped confirmation; database retained")
        server_a, port_a = _relaunch(
            launch, server_a, port_a, db_a, peers_a, proxies, observations
        )
        plan.release()
        if message_count(db_b, INC) != 1 or wait_visible(port_b, "base+inc", 15) != "base+inc":
            observations.extend(_dumps(db_a, db_b, db_c))
            return [failover,
                    _case("cluster_fault", "fail", "A 重启后 B 上的增量被重放成了另一个值。", commands, observations)]
        deadline = time.monotonic() + 15
        bit = ack_bit(db_a, INC, 2)
        while time.monotonic() < deadline and bit != 1:
            time.sleep(0.25)
            bit = ack_bit(db_a, INC, 2)
        observations.append(f"ack_b_after_restart={bit} inc_rows_b={message_count(db_b, INC)}")
        if bit != 1 or message_count(db_b, INC) != 1:
            observations.extend(_dumps(db_a, db_b, db_c))
            observations.extend(_frame_notes(plan))
            return [
                failover,
                _case("cluster_fault", "fail", "重启后的重试没有恰好确认一次，或增量被再次插入。", commands,
                      observations),
            ]

        code, out, err = cli(["--request-id", str(DELETE), f"127.0.0.1:{port_a}", "delete", "acc", "cfg"])
        observations.append(f"delete exit={code} stdout={out!r} stderr={err.strip()!r}")
        if code != 0 or wait_visible(port_b, None, 15) is not None or not journal_has(db_b, INC):
            observations.extend(_dumps(db_a, db_b, db_c))
            return [
                failover,
                _case("cluster_fault", "fail", "删除没有在遮蔽可见值的同时把增量留在 B 的日志里。", commands,
                      observations),
            ]

        resumed = _subscribe(client_bin, state, port_b, work, env, children, commands)
        saw_inc = resumed.wait_for("base+inc", 8, "stdout") or resumed.wait_for(str(INC), 2, "stderr")
        saw_delete = resumed.wait_for("<deleted>", 8, "stdout")
        before_dup = resumed.text("stdout").count("base+inc")
        code, out, err = cli(
            ["--request-id", str(INC), f"127.0.0.1:{port_a}", "incremental", "acc", "cfg", str(BASE), "add_entry",
             "+inc"]
        )
        observations.append(f"duplicate incremental exit={code} stdout={out!r} stderr={err.strip()!r}")
        time.sleep(1.2)
        after_dup = resumed.text("stdout").count("base+inc")
        observations.append("resume stdout:\n" + resumed.text("stdout"))
        observations.append("resume stderr:\n" + resumed.text("stderr"))
        resumed.kill()
        if not saw_inc or not saw_delete or code != 0 or after_dup != before_dup or message_count(db_b, INC) != 1:
            observations.extend(_dumps(db_a, db_b, db_c))
            return [
                failover,
                _case("cluster_fault", "fail", "离线订阅者的补发或重复增量的幂等没有同时成立。", commands, observations),
            ]
        if wait_visible(port_b, None, 5) is not None:
            return [failover, _case("cluster_fault", "fail", "重复增量把已删除的值又写了回来。", commands, observations)]

        before_accepts = tuple(proxy.accepts for proxy in proxies if proxy.name in {"ac", "ca", "bc", "cb"})
        for proxy in proxies:
            if proxy.name in {"ac", "ca", "bc", "cb"}:
                proxy.set_refusing(False)
        if wait_visible(port_c, None, 20) is not None or not journal_has(db_c, INC):
            observations.append(f"c-proxy accepts before heal={before_accepts}")
            observations.extend(_dumps(db_a, db_b, db_c))
            observations.extend(_frame_notes(plan))
            return [
                failover,
                _case("cluster_fault", "fail", "分区愈合后 C 没有在删除已经获胜的情况下补到那条增量。", commands,
                      observations),
            ]
        skew_hlc = message_hlc(db_c, SKEW)
        delete_hlc = message_hlc(db_c, DELETE)
        observations.append(f"skew_hlc={skew_hlc} delete_hlc={delete_hlc}")
        observations.append(
            f"c-proxy accepts before={before_accepts} after="
            + str(tuple(proxy.accepts for proxy in proxies if proxy.name in {"ac", "ca", "bc", "cb"}))
        )
        observations.extend(_dumps(db_a, db_b, db_c))
        observations.extend(_frame_notes(plan))
        if skew_hlc is None or delete_hlc is None or skew_hlc >= delete_hlc:
            return [
                failover,
                _case("cluster_fault", "fail", "C 上的偏移写入不是以更低的 HLC 留在日志里。", commands, observations),
            ]
        if wait_visible(port_a, None, 8) is not None or message_count(db_a, INC) != 1 or message_count(db_c, INC) != 1:
            return [failover,
                    _case("cluster_fault", "fail", "愈合后的三个节点没有收敛到同一次增量加最终删除。", commands,
                          observations)]
        return [
            failover,
            _case(
                "cluster_fault",
                "pass",
                "增量在部分节点持久化、确认帧被精确丢弃、源节点带库重启、分区愈合和离线订阅者补发之后只存在一份；删除遮蔽了可见值，但没有抹掉增量。",
                commands,
                observations,
            ),
        ]
    except Exception:
        observations.append(traceback.format_exc())
        observations.extend(_safe_dumps(db_a, db_b, db_c))
        for child in children:
            if child.proc.poll() is not None:
                observations.append(f"exit {cmdline(child.args)} -> {child.proc.returncode}\n{child.text()[-2000:]}")
        return [_case("cluster_fault", "fail", "集群场景在到达断言前中断。", commands, observations)]
    finally:
        cleanup()


def _failover(cli, dead: int, port_b: int, commands: list[str], observations: list[str]) -> dict:
    code, out, err = cli(
        ["--verbose", f"127.0.0.1:{dead},127.0.0.1:{port_b}", "get", "acc", "cfg"],
        12,
    )
    observations.append(f"failover exit={code} stdout={out!r} stderr={err.strip()!r}")
    connected = f"127.0.0.1:{port_b}" in err
    if code == 0 and out == "base\n" and connected:
        return _case(
            "node_failover",
            "pass",
            "客户端按给定端点顺序跳过拒绝连接的节点，连到下一个存活节点。",
            commands[-1:],
            [err.strip()],
        )
    return _case("node_failover", "fail", "端点故障转移没有落到预期的存活节点。", commands[-1:], [out, err])


def _subscribe(client_bin: Path, state: Path, port: int, work: Path, env: dict[str, str], children: list[Child],
               commands: list[str]) -> Child:
    args = [
        str(client_bin),
        "--state",
        str(state),
        "--timeout-ms",
        "20000",
        f"127.0.0.1:{port}",
        "subscribe",
        "acc",
        "cfg",
    ]
    child = Child(args, work, env)
    children.append(child)
    commands.append(cmdline(args))
    return child


def _relaunch(launch, previous: Child, port: int, db: Path, peers: str, proxies: list[Proxy],
              observations: list[str]) -> tuple[Child, int]:
    previous.kill()
    try:
        return launch(1, port, db, peers, 0), port
    except RuntimeError as error:
        observations.append(f"reuse port {port} failed: {error}")
        from cipple_acceptance.runtime import reserve_ports

        new_port = reserve_ports(1)[0]
        for proxy in proxies:
            if proxy.name in {"ba", "ca"}:
                proxy.set_upstream(new_port)
        observations.append(f"A restarted on {new_port}; proxies ba/ca retargeted")
        return launch(1, new_port, db, peers, 0), new_port


def _dumps(*paths: Path) -> list[str]:
    return [format_db(path) for path in paths]


def _safe_dumps(*paths: Path) -> list[str]:
    rendered = []
    for path in paths:
        try:
            rendered.append(format_db(path))
        except OSError as error:
            rendered.append(f"{path}: {error}")
    return rendered


def _frame_notes(plan: DropPlan) -> list[str]:
    if not plan.notes:
        return ["proxy saw no sync or ack frames"]
    return [
        f"proxy {note.proxy} {note.direction} type={note.msg_type} rpc={note.rpc} {note.detail} dropped={note.dropped}"
        for note in plan.notes
    ]
