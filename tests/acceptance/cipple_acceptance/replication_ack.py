"""Retry persisting a received ACK while keeping the entity out of the send queue."""

from __future__ import annotations

import sqlite3
import time
import uuid
from contextlib import ExitStack, closing
from pathlib import Path

from cipple_acceptance.protocol import Conn, PUT_RESPONSE, SYNC_RESPONSE, decode_bool_response, put_request
from cipple_acceptance.proxy import DropPlan, Proxy
from cipple_acceptance.runtime import Child, cmdline, reserve_ports
from cipple_acceptance.sqlite_obs import ack_bit, message_count


def run_replication_ack(server_bin: Path, _client_bin: Path, work: Path, env: dict[str, str]) -> list[dict]:
    work.mkdir(parents=True, exist_ok=True)
    source_port, peer_port, proxy_port = reserve_ports(3)
    source_db, peer_db = work / "source.db", work / "peer.db"
    entity = uuid.uuid4()
    trace = DropPlan(entity, drop_first=False)
    commands: list[str] = []
    observations: list[str] = []

    with ExitStack() as processes:
        proxy = Proxy("ack-retry", proxy_port, peer_port, trace)
        processes.callback(proxy.close)

        def launch(port: int, db: Path, node: int, peers: str) -> None:
            args = [str(server_bin), str(port), str(db), "--id", str(node), "--peers", peers, "--overlay", "star"]
            child = Child(args, work, env)
            commands.append(cmdline(args))
            processes.callback(child.kill)
            if not child.wait_for("Listening on", 8, "stdout"):
                raise RuntimeError(f"服务端没有开始监听。{child.text()}")

        launch(peer_port, peer_db, 2, f"1@127.0.0.1:{source_port}")
        launch(source_port, source_db, 1, f"2@127.0.0.1:{proxy_port}")
        with closing(sqlite3.connect(source_db, timeout=5)) as db:
            with db:
                db.execute("""
                    CREATE TRIGGER fail_ack_update BEFORE UPDATE OF acknowledged ON ack
                    WHEN NEW.acknowledged = 1
                    BEGIN SELECT RAISE(ABORT, 'acceptance blocks local acknowledgement'); END
                """)

            with closing(Conn("127.0.0.1", source_port)) as writer:
                response = writer.request(put_request(entity, b"replication", b"ack", b"stored"))
                if not decode_bool_response(response, PUT_RESPONSE):
                    raise RuntimeError("源节点拒绝初始化写入。")

            received = False
            deadline = time.monotonic() + 8
            while not received and time.monotonic() < deadline:
                with trace.lock:
                    received = any(note.msg_type == SYNC_RESPONSE and note.detail == "result=0" for note in trace.notes)
                if not received:
                    time.sleep(0.025)

            # Allow the original send deadline to expire while local ACK writes fail.
            if received:
                time.sleep(2.25)
            pending = ack_bit(source_db, entity, 2) == 0
            with db:
                db.execute("DROP TRIGGER fail_ack_update")

        deadline = time.monotonic() + 8
        while time.monotonic() < deadline and ack_bit(source_db, entity, 2) != 1:
            time.sleep(0.1)
        acknowledged = ack_bit(source_db, entity, 2) == 1
        stored_once = message_count(peer_db, entity) == 1
        with trace.lock:
            sends = len(trace.rpc_ids)

        passed = received and pending and acknowledged and stored_once and sends == 1
        observations.append(f"replica_ack_received={received} ack_pending_during_failure={pending}")
        observations.append(f"sync_requests={sends} ack_recovered={acknowledged} stored_once={stored_once}")
        return [{
            "name": "replication_ack_commit_retry",
            "status": "pass" if passed else "fail",
            "detail": (
                "收到副本回执后，源节点在本地重试保存确认，未再次发送同一版本。"
                if passed else "源节点未能在本地恢复确认写入，或重复发送了已获得持久回执的版本。"
            ),
            "commands": commands,
            "observations": observations,
        }]
