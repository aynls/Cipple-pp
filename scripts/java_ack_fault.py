"""Run two Ripple processes and test source ACK when a relay discards SYNC."""

import argparse
import contextlib
import json
from pathlib import Path
import re
import select
import socket
import socketserver
import sqlite3
import struct
import subprocess
import tempfile
import threading
import time
import uuid

from demo import reserve_ports, stop, wait_listening


class RelayHandler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            with socket.create_connection(("127.0.0.1", self.server.upstream), timeout=1) as peer:
                sockets = [self.request, peer]
                while not self.server.done.is_set():
                    readable, _, _ = select.select(sockets, [], [], 0.2)
                    for incoming in readable:
                        data = incoming.recv(65536)
                        if not data:
                            return
                        if incoming is self.request and self.server.block.is_set():
                            with self.server.lock:
                                self.server.discarded.extend(data)
                        else:
                            (peer if incoming is self.request else self.request).sendall(data)
        except OSError:
            return


class Relay(socketserver.ThreadingTCPServer):
    daemon_threads = True

    def __init__(self, upstream):
        self.upstream = upstream
        self.done = threading.Event()
        self.block = threading.Event()
        self.lock = threading.Lock()
        self.discarded = bytearray()
        super().__init__(("127.0.0.1", 0), RelayHandler)

    def finish(self):
        self.done.set()
        self.shutdown()


def wait_for(operation, description, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = operation()
        if result:
            return result
        time.sleep(0.05)
    raise RuntimeError(f"Timed out waiting for {description}")


def field(value):
    raw = value.encode("utf-8")
    return struct.pack(">i", len(raw)) + raw


def put(port, value):
    correlation = str(uuid.uuid4())
    request = bytes([10]) + field(correlation) + field("ack-fault") + field("key") + field(value)
    expected = bytes([11]) + field(correlation) + bytes([1])
    with socket.create_connection(("127.0.0.1", port), timeout=5) as writer:
        writer.sendall(struct.pack(">i", len(request)) + request)
        with writer.makefile("rb") as response:
            length = struct.unpack(">i", response.read(4))[0]
            actual = response.read(length)
    if actual != expected:
        raise RuntimeError(f"Ripple did not confirm the write: {actual.hex()}")


def query(database, sql, args=()):
    with contextlib.closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True, timeout=2)) as db:
        return db.execute(sql, args).fetchone()


def exercise(jar, directory):
    a_port, b_port, a_ui, b_ui = reserve_ports(4)
    a_db = directory / "node-a.db"
    b_db = directory / "node-b.db"
    a_log = directory / "node-a.log"
    marker = "sync-discarded-" + uuid.uuid4().hex

    def start(node, port, ui, database, destination_b, log):
        args = [
            "java", "-XX:ActiveProcessorCount=4", f"-Did={node}", "-Daddress=127.0.0.1", f"-DapiPort={port}",
            f"-DuiPort={ui}", f"-DstorageLocation={database}", f"-Dnodes=1:127.0.0.1:{a_port},2:127.0.0.1:{destination_b}",
            "-Dprotocol=star", "-jar", str(jar),
        ]
        output = stack.enter_context(log.open("wb"))
        process = subprocess.Popen(args, cwd=jar.parent, stdout=output, stderr=subprocess.STDOUT)
        stack.callback(stop, process)
        wait_listening(port, process)

    with contextlib.ExitStack() as stack:
        relay = stack.enter_context(Relay(b_port))
        threading.Thread(target=relay.serve_forever, daemon=True).start()
        stack.callback(relay.finish)
        relay_port = relay.server_address[1]
        start(2, b_port, b_ui, b_db, b_port, directory / "node-b.log")
        start(1, a_port, a_ui, a_db, relay_port, a_log)
        heartbeat = f"Set liveness of node 127.0.0.1:{relay_port} to true."
        wait_for(lambda: heartbeat in a_log.read_text(encoding="utf-8", errors="replace"), "peer heartbeat")

        relay.block.set()
        put(a_port, marker)
        message_id = wait_for(
            lambda: query(a_db, "SELECT uuid FROM message WHERE new_value = ?", (marker,)), "source message",
        )[0]

        def source_ack():
            row = query(a_db, "SELECT ack_nodes FROM ack WHERE message_uuid = ?", (message_id,))
            if row and 2 in map(int, re.findall(r"\d+", row[0] or "")):
                return row[0]
            return None

        acked = wait_for(source_ack, "source marking destination 2 acknowledged", timeout=5)
        wait_for(lambda: marker.encode() in relay.discarded, "relay discarding the real SYNC")
        before = query(b_db, "SELECT COUNT(*) FROM message WHERE uuid = ?", (message_id,))[0]

        healed_at = time.monotonic()
        relay.block.clear()
        time.sleep(12)
        after = query(b_db, "SELECT COUNT(*) FROM message WHERE uuid = ?", (message_id,))[0]
        result = {
            "expected_defect_reproduced": before == 0 and after == 0,
            "write_response_success": True,
            "source_ack_nodes": acked,
            "entity_uuid": message_id,
            "discarded_bytes": len(relay.discarded),
            "peer_count_before_heal": before,
            "peer_count_after_heal": after,
            "seconds_after_heal": round(time.monotonic() - healed_at, 3),
        }
        (directory / "result.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result, indent=2))
        return result["expected_defect_reproduced"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-jar", type=Path, required=True, help="Ripple standalone server JAR with sibling lib/")
    parser.add_argument("--output", type=Path, help="Parent directory for results")
    args = parser.parse_args()
    if args.output:
        args.output.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix="ripple-ack-", dir=args.output)).resolve()
    print(f"Logs and databases: {directory}", flush=True)
    raise SystemExit(0 if exercise(args.server_jar.resolve(), directory) else 1)
