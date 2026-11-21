"""A replica that rejects old versions can still accept later versions."""

from __future__ import annotations

import sqlite3
import time
import uuid
from contextlib import ExitStack, closing
from pathlib import Path

from cipple_acceptance.protocol import Conn, PUT_RESPONSE, decode_bool_response, put_request
from cipple_acceptance.runtime import Child, cmdline, reserve_ports
from cipple_acceptance.sqlite_obs import ack_bit, message_count


def run_replication_progress(server_bin: Path, _client_bin: Path, work: Path, env: dict[str, str]) -> list[dict]:
    work.mkdir(parents=True, exist_ok=True)
    source_port, peer_port = reserve_ports(2)
    source_db, peer_db = work / "source.db", work / "peer.db"
    older_count = 256
    later = uuid.UUID(int=older_count + 1)
    commands: list[str] = []

    with ExitStack() as processes:
        def launch(port: int, path: Path, *options: str) -> Child:
            args = [str(server_bin), str(port), str(path), *options]
            commands.append(cmdline(args))
            child = Child(args, work, env)
            processes.callback(child.kill)
            if not child.wait_for("Listening on", 8, "stdout"):
                raise RuntimeError(f"服务端没有开始监听。{child.text()}")
            return child

        # Prepare the receiver without exposing its cluster endpoint yet.
        launch(0, peer_db).kill()
        with closing(sqlite3.connect(peer_db)) as db:
            with db:
                db.execute("""
                           CREATE TRIGGER reject_old_replication
                               BEFORE INSERT
                               ON message
                               WHEN NEW.value = X'6F6C64'
                           BEGIN
                               SELECT RAISE(ABORT, 'acceptance rejects old version');
                           END
                           """)

        launch(source_port, source_db, "--id", "1", "--peers", f"2@127.0.0.1:{peer_port}", "--overlay", "star")
        with closing(Conn("127.0.0.1", source_port)) as writer:
            for index in range(1, older_count + 2):
                entity = uuid.UUID(int=index)
                value = b"new" if entity == later else b"old"
                response = writer.request(put_request(entity, b"replication", b"progress", value))
                if not decode_bool_response(response, PUT_RESPONSE):
                    raise RuntimeError(f"源节点拒绝初始化版本 {index}。")

        peer = launch(peer_port, peer_db, "--id", "2", "--peers", f"1@127.0.0.1:{source_port}", "--overlay", "star")
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline and ack_bit(source_db, later, 2) != 1 and peer.proc.poll() is None:
            time.sleep(0.1)

        older_pending = ack_bit(source_db, uuid.UUID(int=1), 2) == 0
        later_acked = ack_bit(source_db, later, 2) == 1
        later_stored = message_count(peer_db, later) == 1
        rejections = peer.text("stderr").count("acceptance rejects old version")
        passed = older_pending and later_acked and later_stored and rejections > 0
        return [{
            "name": "replication_progress_past_rejections",
            "status": "pass" if passed else "fail",
            "detail": (
                "接收节点持续拒绝旧版本时，第 257 个版本仍在对端落盘并获得确认。"
                if passed else "第 257 个版本未在指定的复制故障场景中同时落盘和确认。"
            ),
            "commands": commands,
            "observations": [
                f"old_versions={older_count} peer_rejections={rejections} oldest_pending={older_pending}",
                f"version_257_stored={later_stored} version_257_acked={later_acked}",
            ],
        }]
