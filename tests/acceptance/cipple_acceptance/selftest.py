"""Checks that do not need the candidate binary."""

from __future__ import annotations

import socket
import struct
import threading
import uuid

from cipple_acceptance.oracle import check_oracle
from cipple_acceptance.protocol import SYNC_RESPONSE, encode_entity, sync_request, sync_response
from cipple_acceptance.proxy import DropPlan, Proxy
from cipple_acceptance.runtime import reserve_ports

ENTITY = uuid.UUID("00000000-0000-4000-8000-0000000000e1")
RPC = uuid.UUID("00000000-0000-4000-8000-0000000000e2")


def oracle_case() -> dict:
    notes = check_oracle()
    return {
        "name": "oracle_contract",
        "status": "pass",
        "detail": "纯函数预言覆盖增量折叠、删除遮蔽、重复 UUID 和 (HLC, server id, UUID) 排序。",
        "commands": [],
        "observations": notes,
    }


def _read_frame(sock: socket.socket) -> bytes:
    header = b""
    while len(header) < 4:
        chunk = sock.recv(4 - len(header))
        if not chunk:
            raise EOFError("upstream closed")
        header += chunk
    length = struct.unpack(">i", header)[0]
    body = b""
    while len(body) < length:
        chunk = sock.recv(length - len(body))
        if not chunk:
            raise EOFError("upstream closed inside frame")
        body += chunk
    return body


def _serve(port: int, stop: threading.Event, seen: list[bytes]) -> None:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", port))
    listener.listen(8)
    listener.settimeout(0.2)
    try:
        while not stop.is_set():
            try:
                conn, _addr = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            with conn:
                conn.settimeout(2)
                try:
                    body = _read_frame(conn)
                except (EOFError, ConnectionResetError, TimeoutError, OSError):
                    # The proxy can close the upstream before a request arrives.
                    continue
                seen.append(body)
                try:
                    rpc = uuid.UUID(bytes=body[1:17])
                    conn.sendall(sync_response(rpc, 0))
                    conn.settimeout(1)
                    try:
                        _read_frame(conn)
                    except (EOFError, ConnectionResetError, TimeoutError, OSError):
                        pass
                except (EOFError, ConnectionResetError, TimeoutError, OSError):
                    pass
    finally:
        listener.close()


def proxy_case() -> dict:
    upstream_port, proxy_port = reserve_ports(2)
    stop = threading.Event()
    seen: list[bytes] = []
    thread = threading.Thread(target=_serve, args=(upstream_port, stop, seen), daemon=True)
    thread.start()
    plan = DropPlan(ENTITY)
    proxy = Proxy("self", proxy_port, upstream_port, plan)
    observations = [f"upstream={upstream_port}", f"proxy={proxy_port}"]
    try:
        entity = encode_entity(ENTITY, "update", b"app", b"key", value=b"base", hlc=10, server_id=1)
        raw = sync_request(RPC, entity, 1)
        client = socket.create_connection(("127.0.0.1", proxy_port), 3)
        client.settimeout(1.5)
        client.sendall(raw)
        dropped_cleanly = False
        try:
            _read_frame(client)
        except (TimeoutError, EOFError, ConnectionResetError, ConnectionAbortedError, OSError):
            # A withheld confirmation may time out, or the proxy may close (EOF / reset).
            dropped_cleanly = True
        finally:
            client.close()
        observations.append(f"first response withheld={dropped_cleanly}")
        observations.append(f"upstream saw {len(seen)} request(s)")
        observations.append(f"dropped={plan.dropped} refusing={plan.refusing}")
        if not dropped_cleanly or plan.dropped != 1 or not seen:
            return _fail("代理没有精确丢弃第一次持久化确认。", observations)
        plan.release()
        client = socket.create_connection(("127.0.0.1", proxy_port), 3)
        try:
            client.settimeout(3)
            client.sendall(raw)
            body = _read_frame(client)
        finally:
            client.close()
        observations.append(f"second type={body[0]} result={body[17] if len(body) > 17 else 'missing'}")
        if body[0] != SYNC_RESPONSE or body[17] != 0 or plan.dropped != 1:
            return _fail("恢复转发后确认帧没有到达，或再次被丢弃。", observations)
        observations.append("第二次 SyncResponse 被转发，丢弃计数保持为 1。")
        return {
            "name": "proxy_drop_once",
            "status": "pass",
            "detail": "本地代理能在转发 SyncRequest 之后丢弃恰好一帧成功的 SyncResponse，然后恢复转发。",
            "commands": [],
            "observations": observations,
        }
    finally:
        proxy.close()
        stop.set()
        thread.join(timeout=2)


def _fail(detail: str, observations: list[str]) -> dict:
    return {
        "name": "proxy_drop_once",
        "status": "fail",
        "detail": detail,
        "commands": [],
        "observations": observations,
    }
