"""Mesh convergence after the source dies.

A writes one version and waits until B has stored it. C is partitioned for the
whole of that interval. A is then killed and left dead. Only the B–C proxies
are opened. C must converge to B's copy, and A's unacknowledged row for C must
stay unacknowledged.
"""

from __future__ import annotations

import time
import traceback
import uuid
from pathlib import Path

from cipple_acceptance.proxy import Proxy
from cipple_acceptance.runtime import Child, cmdline, reserve_ports
from cipple_acceptance.sqlite_obs import ack_bit, format_db, journal_has, message_count

MSG = uuid.UUID("00000000-0000-4000-8000-0000000000b1")
APP = "mesh"
KEY = "held"
VALUE = "mesh-held"


def _case(name: str, status: str, detail: str, commands: list[str], observations: list[str]) -> dict:
    return {
        "name": name,
        "status": status,
        "detail": detail,
        "commands": list(commands),
        "observations": list(observations),
    }


def run_mesh(server_bin: Path, client_bin: Path, work: Path, env: dict[str, str]) -> list[dict]:
    work.mkdir(parents=True, exist_ok=True)
    port_a, port_b, port_c, p_ab, p_ac, p_ba, p_bc, p_ca, p_cb = reserve_ports(9)
    db_a, db_b, db_c = work / "a.db", work / "b.db", work / "c.db"
    proxies = [
        Proxy("ab", p_ab, port_b),
        Proxy("ba", p_ba, port_a),
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
        f"ports A={port_a} B={port_b} C={port_c}",
        f"proxies ab={p_ab} ba={p_ba} ac={p_ac} ca={p_ca} bc={p_bc} cb={p_cb}",
        "C 从启动起被四条代理隔离。A 只在 B 落盘后被终止，之后不再启动。",
    ]

    def cleanup() -> None:
        for child in children:
            child.kill()
        for proxy in proxies:
            proxy.close()

    def launch(node_id: int, port: int, db: Path, peers: str) -> Child:
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
            "mesh",
            "--clock-offset-ms",
            "0",
        ]
        child = Child(args, work, env)
        children.append(child)
        commands.append(cmdline(args))
        if not child.wait_for("Listening on", 8, "stdout"):
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
        code, out, err = cli([f"127.0.0.1:{port}", "get", APP, KEY])
        if code == 0:
            return out.rstrip("\n")
        if code == 1 and "Configuration not found" in err:
            return None
        raise RuntimeError(f"get 127.0.0.1:{port} exit={code} stdout={out!r} stderr={err.strip()!r}")

    def wait_stored(timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if visible(port_b) == VALUE and message_count(db_b, MSG) == 1 and ack_bit(db_a, MSG, 2) == 1:
                return True
            time.sleep(0.25)
        return False

    try:
        peers_a = f"2@127.0.0.1:{p_ab},3@127.0.0.1:{p_ac}"
        peers_b = f"1@127.0.0.1:{p_ba},3@127.0.0.1:{p_bc}"
        peers_c = f"1@127.0.0.1:{p_ca},2@127.0.0.1:{p_cb}"
        server_a = launch(1, port_a, db_a, peers_a)
        launch(2, port_b, db_b, peers_b)
        launch(3, port_c, db_c, peers_c)
        code, out, err = cli(["--request-id", str(MSG), f"127.0.0.1:{port_a}", "put", APP, KEY, VALUE])
        observations.append(f"put exit={code} stdout={out!r} stderr={err.strip()!r}")
        if code != 0 or not wait_stored(20):
            observations.extend(_safe_dumps(db_a, db_b, db_c))
            return [_case("mesh_survivor", "fail", "A 仍存活时，B 没有落盘并确认这一版本。", commands, observations)]
        if journal_has(db_c, MSG) or visible(port_c) is not None:
            observations.extend(_safe_dumps(db_a, db_b, db_c))
            return [_case("mesh_survivor", "fail", "C 在隔离期间已经拿到了版本。", commands, observations)]

        pid = server_a.proc.pid
        server_a.kill()
        observations.append(f"killed A pid={pid} exit={server_a.proc.poll()}; A stays down")
        if server_a.proc.poll() is None:
            return [_case("mesh_survivor", "fail", "A 没有退出。", commands, observations)]
        for proxy in proxies:
            if proxy.name in {"ab", "ba", "ac", "ca"}:
                proxy.set_refusing(True)
            elif proxy.name in {"bc", "cb"}:
                proxy.set_refusing(False)
        observations.append("reopened only B-C; A-B and A-C stay refused")

        deadline = time.monotonic() + 25
        seen = visible(port_c)
        while time.monotonic() < deadline and seen != VALUE:
            time.sleep(0.25)
            seen = visible(port_c)
        bit_c = ack_bit(db_b, MSG, 3)
        ack_deadline = time.monotonic() + 8
        while time.monotonic() < ack_deadline and bit_c != 1 and seen == VALUE:
            time.sleep(0.25)
            bit_c = ack_bit(db_b, MSG, 3)
        bit_a = ack_bit(db_a, MSG, 3)
        observations.append(
            f"C visible={seen!r} ack_b_for_c={bit_c} ack_a_for_c={bit_a} "
            f"rows A/B/C={message_count(db_a, MSG)}/{message_count(db_b, MSG)}/{message_count(db_c, MSG)} "
            f"A exit={server_a.proc.poll()}"
        )
        observations.extend(_safe_dumps(db_a, db_b, db_c))
        if (
            seen == VALUE
            and bit_c == 1
            and bit_a == 0
            and message_count(db_a, MSG) == 1
            and message_count(db_b, MSG) == 1
            and message_count(db_c, MSG) == 1
            and server_a.proc.poll() is not None
            and visible(port_b) == VALUE
        ):
            return [
                _case(
                    "mesh_survivor",
                    "pass",
                    "A 在 B 落盘后保持死亡，只接通 B 与 C 后两边收敛到同一版本；A 对 C 的确认仍是 0。",
                    commands,
                    observations,
                )
            ]
        return [
            _case(
                "mesh_survivor",
                "fail",
                "A 保持死亡且只恢复 B-C 之后，剩余节点没有收敛到 B 已持有的那一份。",
                commands,
                observations,
            )
        ]
    except Exception:
        observations.append(traceback.format_exc())
        observations.extend(_safe_dumps(db_a, db_b, db_c))
        return [_case("mesh_survivor", "fail", "mesh 场景在到达断言前中断。", commands, observations)]
    finally:
        cleanup()


def _safe_dumps(*paths: Path) -> list[str]:
    rendered = []
    for path in paths:
        try:
            rendered.append(format_db(path))
        except OSError as error:
            rendered.append(f"{path}: {error}")
    return rendered
