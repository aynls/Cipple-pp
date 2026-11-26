"""Exercise quorum, failover and snapshot catch-up using separate Windows processes."""

from __future__ import annotations

import sqlite3
import time
import traceback
import uuid
from contextlib import closing
from pathlib import Path

from cipple_acceptance.protocol import (
    GET_RESPONSE,
    HISTORY_LOST_RESPONSE,
    OP_UPDATE,
    PUT_RESPONSE,
    SUBSCRIBE_RESPONSE,
    Conn,
    ProtocolError,
    decode_body,
    decode_bool_response,
    decode_dispatch,
    decode_get,
    decode_retry,
    encode_entity,
    get_request,
    put_request,
    subscribe_request,
    sync_request,
    sync_result,
)
from cipple_acceptance.runtime import Child, cmdline, reserve_ports

APP = b"raft"


def eventually(probe, detail: str, timeout: float = 15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = probe()
        if result is not None and result is not False:
            return result
        time.sleep(0.05)
    raise AssertionError(detail)


def rows(path: Path, sql: str, parameters=()):
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)) as db:
        return db.execute(sql, parameters).fetchall()


class Cluster:
    def __init__(self, server: Path, client: Path, directory: Path, env: dict[str, str]):
        directory.mkdir(parents=True)
        self.server, self.client, self.directory, self.env = server, client, directory, env
        ports = reserve_ports(6)
        self.app_ports, self.raft_ports = ports[:3], ports[3:]
        self.nodes: dict[int, Child] = {}
        self.commands: list[str] = []
        self.observations: list[str] = []

    def db(self, node: int) -> Path:
        return self.directory / f"node-{node}.db"

    def command(self, args: list[str]) -> Child:
        self.commands.append(cmdline(args))
        return Child(args, self.directory, self.env)

    def start(self, node: int) -> None:
        peers = ",".join(f"{peer + 1}@127.0.0.1:{port}"
                         for peer, port in enumerate(self.raft_ports) if peer != node)
        child = self.command([
            str(self.server), str(self.app_ports[node]), str(self.db(node)), "--id", str(node + 1),
            "--raft-port", str(self.raft_ports[node]), "--peers", peers, "--snapshot-distance", "16",
        ])
        self.nodes[node] = child
        assert child.wait_for("Listening on", 10), child.text()

    def stop(self, node: int) -> None:
        child = self.nodes.pop(node)
        child.kill()
        if child.text("stderr"):
            self.observations.append(f"node {node + 1} stderr\n{child.text('stderr')}")

    def close(self) -> None:
        for node in list(self.nodes):
            self.stop(node)

    def connect(self, node: int, timeout: float = 5) -> Conn:
        return Conn("127.0.0.1", self.app_ports[node], timeout)

    def request(self, node: int, message: bytes) -> bytes:
        conn = self.connect(node)
        try:
            return conn.request(message)
        finally:
            conn.close()

    def leader(self) -> int:
        def find():
            for node, child in self.nodes.items():
                assert child.proc.poll() is None, child.text()
                try:
                    reply = self.request(node, get_request(uuid.uuid4(), APP, b"probe"))
                except (OSError, EOFError):
                    continue
                if reply[0] == GET_RESPONSE:
                    return node
            return None

        return eventually(find, "no node could commit a linearizable read")

    def start_all(self) -> None:
        for node in range(3):
            self.start(node)

    def endpoints(self, first: int) -> str:
        nodes = [first] + [node for node in range(3) if node != first]
        return ",".join(f"127.0.0.1:{self.app_ports[node]}" for node in nodes)

    def cli(self, first: int, command: str, *fields: str, request_id: uuid.UUID | None = None):
        args = [str(self.client), "--timeout-ms", "7000"]
        if request_id is not None:
            args += ["--request-id", str(request_id)]
        child = self.command([*args, self.endpoints(first), command, "raft", "switch", *fields])
        code = child.finish(10)
        self.observations.append(f"{command} exit={code}\n{child.text()}")
        return code, child.text("stdout"), child.text("stderr")

    def event(self, node: int, request_id: uuid.UUID) -> int | None:
        events = rows(self.db(node), "SELECT sequence FROM delivery_log WHERE message_uuid=?", (request_id.bytes,))
        return events[0][0] if events else None


def failover(cluster: Cluster) -> None:
    cluster.start_all()
    first = cluster.leader()
    first_follower = next(node for node in cluster.nodes if node != first)
    spare = next(node for node in cluster.nodes if node not in (first, first_follower))
    initial = uuid.uuid4()
    state = cluster.directory / "subscriber.db"
    watcher = cluster.command([str(cluster.client), "--state", str(state), "--timeout-ms", "8000",
                               cluster.endpoints(first), "subscribe", "raft", "switch"])
    try:
        assert watcher.wait_for("Subscribed", 10, "stdout"), watcher.text()

        previous = uuid.uuid4()
        assert cluster.cli(first, "put", "before-failover", request_id=previous)[:2] == (0, "OK\n")
        assert watcher.wait_for("before-failover", 10, "stdout"), watcher.text()
        sequences = [eventually(lambda: cluster.event(node, previous), f"node {node + 1} did not apply")
                     for node in cluster.nodes]
        assert len(set(sequences)) == 1, sequences

        # Acknowledge a new write while only a bare majority is running.
        cluster.stop(spare)
        probe = uuid.uuid4()
        decode_retry(cluster.request(first_follower, get_request(probe, APP, b"switch")), probe)
        assert cluster.cli(first_follower, "put", "quorum-v1", request_id=initial)[:2] == (0, "OK\n")
        # The stopped node missed this write entirely. The surviving member must have it durably.
        cluster.stop(first)
        cluster.start(spare)
        second = cluster.leader()
        assert second != first
        cluster.observations.append(f"leader {first + 1} terminated, elected {second + 1}")

        assert cluster.cli(second, "get")[:2] == (0, "quorum-v1\n")
        assert watcher.wait_for("quorum-v1", 10, "stdout"), watcher.text()
        assert cluster.cli(second, "put", "quorum-v1", request_id=initial)[:2] == (0, "OK\n")
        assert cluster.cli(second, "put", "conflict", request_id=initial)[0] != 0
        assert cluster.cli(second, "get")[:2] == (0, "quorum-v1\n")
        assert cluster.cli(second, "put", "quorum-v2")[:2] == (0, "OK\n")
        assert watcher.wait_for("quorum-v2", 10, "stdout"), watcher.text()
        assert watcher.text("stdout").splitlines().count("quorum-v1") == 1, watcher.text()
        assert watcher.text("stdout").splitlines().count("before-failover") == 1, watcher.text()

        # Keep the elected leader's application socket reachable after losing the majority.
        last_follower = next(node for node in cluster.nodes if node != second)
        cluster.stop(last_follower)
        isolated = uuid.uuid4()
        decode_retry(cluster.request(second, put_request(isolated, APP, b"unconfirmed", b"minority")), isolated)
        probe = uuid.uuid4()
        decode_retry(cluster.request(second, get_request(probe, APP, b"switch")), probe)
        assert cluster.event(second, isolated) is None, "minority entry reached the state machine"

        legacy_id = uuid.uuid4()
        encoded = encode_entity(legacy_id, OP_UPDATE, APP, b"old-sync", value=b"minority", server_id=first + 1)
        assert sync_result(cluster.request(second, sync_request(uuid.uuid4(), encoded, first + 1))) == 2
        assert cluster.event(second, legacy_id) is None, "legacy SyncRequest bypassed consensus"

        cluster.start(first)
        cluster.start(last_follower)
        assert cluster.cli(cluster.leader(), "get")[:2] == (0, "quorum-v2\n")
    finally:
        watcher.kill()
        cluster.observations.append(watcher.text())


def log_range(path: Path) -> tuple[int, int]:
    # The minimum retained log index must pass the stopped node's last entry.
    minimum, maximum = rows(path, "SELECT MIN(idx), MAX(idx) FROM raft_log")[0]
    return minimum or 0, maximum or 0


def snapshot_catchup(cluster: Cluster) -> None:
    cluster.start_all()
    leader = cluster.leader()
    follower = next(node for node in cluster.nodes if node != leader)
    seed = uuid.uuid4()
    assert decode_bool_response(cluster.request(leader, put_request(seed, APP, b"batch", b"seed")), PUT_RESPONSE)
    seed_index = eventually(lambda: cluster.event(follower, seed), "follower did not apply the seed")
    cluster.stop(follower)
    stopped_index = log_range(Path(str(cluster.db(follower)) + ".raft"))[1]
    assert stopped_index >= seed_index

    publisher = cluster.connect(leader)
    try:
        for index in range(160):
            final = uuid.uuid4()
            reply = publisher.request(put_request(final, APP, b"batch", str(index).encode("ascii")))
            assert decode_bool_response(reply, PUT_RESPONSE), f"write {index} was rejected"
    finally:
        publisher.close()

    expected = cluster.event(leader, final)
    assert expected is not None
    old_suffix = Path(str(cluster.db(leader)) + ".raft")

    def compacted():
        start, end = log_range(old_suffix)
        return (start, end) if start > stopped_index else None

    remaining = eventually(compacted, f"leader retained all logs from offline follower index {stopped_index}")
    cluster.observations.append(f"follower stopped at index {stopped_index}, leader retained {remaining}")
    cluster.start(follower)
    applied = eventually(lambda: cluster.event(follower, final), "follower did not install snapshot and apply tail")
    assert applied == expected, (applied, expected)
    assert rows(cluster.db(follower),
                "SELECT COUNT(*) FROM delivery_log WHERE application_name=? AND key=?", (APP, b"batch")) == [(128,)]

    consumer = cluster.connect(follower)
    try:
        rpc = uuid.uuid4()
        lost = consumer.request(subscribe_request(rpc, APP, b"batch", resume_after=0))
        msg_type, msg_id, fields = decode_body(lost)
        assert (msg_type, msg_id) == (HISTORY_LOST_RESPONSE, rpc)
        floor = fields.i64()
        assert fields.remaining() == 0
        assert floor > seed_index, floor
        response = consumer.request(subscribe_request(uuid.uuid4(), APP, b"batch", resume_after=floor))
        assert decode_bool_response(response, SUBSCRIBE_RESPONSE)
        assert decode_dispatch(consumer.read_frame()).sequence > floor
    finally:
        consumer.close()

    state = cluster.directory / "subscriber.db"
    base = [str(cluster.client), "--state", str(state), "--timeout-ms", "7000"]
    topic = [f"127.0.0.1:{cluster.app_ports[follower]}", "subscribe", "raft", "batch"]
    expired = cluster.command([*base, *topic])
    assert expired.finish(10) == 1 and str(floor) in expired.text("stderr"), expired.text()
    resumed = cluster.command([*base, "--resume-after", str(floor), "--unsubscribe-after", "1", *topic])
    assert resumed.finish(10) == 0 and "Unsubscribed" in resumed.text("stdout"), resumed.text()

    # All processes restart after compaction. The majority can still read and append.
    cluster.close()
    cluster.start_all()
    elected = cluster.leader()
    reply = cluster.request(elected, get_request(uuid.uuid4(), APP, b"batch"))
    assert decode_get(reply).visible == b"159"
    # Replaying the seed after its payload is recycled gives the original result without a new event.
    assert decode_bool_response(cluster.request(elected, put_request(seed, APP, b"batch", b"seed")), PUT_RESPONSE)
    assert cluster.event(elected, seed) is None
    assert decode_get(cluster.request(elected, get_request(uuid.uuid4(), APP, b"batch"))).visible == b"159"


def run_raft(server: Path, client: Path, work: Path, env: dict[str, str]) -> list[dict]:
    cases = []
    for name, run in (("raft_failover", failover), ("raft_snapshot_catchup", snapshot_catchup)):
        cluster = Cluster(server, client, work / name, env)
        status, detail = "pass", "通过"
        try:
            run(cluster)
        except (AssertionError, EOFError, OSError, ProtocolError, RuntimeError, sqlite3.Error):
            status, detail = "fail", traceback.format_exc().strip()
        finally:
            cluster.close()
        cases.append({"name": name, "status": status, "detail": detail,
                      "commands": cluster.commands, "observations": cluster.observations})
    return cases
