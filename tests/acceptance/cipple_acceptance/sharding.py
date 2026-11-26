"""Exercise two independent Raft groups through the static shard layout."""

from __future__ import annotations

import traceback
import uuid
from contextlib import closing
from pathlib import Path

from cipple_acceptance.protocol import (
    ADD_ENTRY,
    Conn,
    decode_body,
    delete_request,
    get_request,
    incremental_request,
    put_request,
    subscribe_request,
    unsubscribe_request,
)
from cipple_acceptance.raft import eventually, rows
from cipple_acceptance.runtime import Child, cmdline, reserve_ports

APP = "sharding"
FIRST_KEY, SECOND_KEY = "item1", "item0"
FIRST_SHARD, SECOND_SHARD = 7, 19
WRONG_SHARD_RESPONSE = 20


class Deployment:
    def __init__(self, server: Path, client: Path, directory: Path, env: dict[str, str]):
        directory.mkdir(parents=True)
        self.server, self.client, self.directory, self.env = server, client, directory, env
        ports = reserve_ports(12)
        self.app_ports, self.raft_ports = ports[:6], ports[6:]
        self.layout = directory / "shards.txt"
        self.layout.write_text(self.layout_text(SECOND_SHARD), encoding="ascii")
        self.nodes: dict[int, Child] = {}
        self.commands: list[str] = []
        self.observations: list[str] = []

    def layout_text(self, second_shard: int) -> str:
        return "# shard member application raft\n" + "".join(
            f"{FIRST_SHARD if node < 3 else second_shard} {node % 3 + 1} "
            f"127.0.0.1:{self.app_ports[node]} 127.0.0.1:{self.raft_ports[node]}\n"
            for node in reversed(range(6))
        )

    def db(self, node: int) -> Path:
        return self.directory / f"node-{node}.db"

    def command(self, args: list[str]) -> Child:
        self.commands.append(cmdline(args))
        return Child(args, self.directory, self.env)

    def server_args(self, node: int, layout: Path) -> list[str]:
        return [str(self.server), str(self.app_ports[node]), str(self.db(node)), "--id", str(node % 3 + 1),
                "--shards", str(layout), "--shard-id", str(FIRST_SHARD if node < 3 else SECOND_SHARD)]

    def start(self, node: int, layout: Path | None = None) -> None:
        child = self.command(self.server_args(node, layout or self.layout))
        self.nodes[node] = child
        assert child.wait_for("Listening on", 10, "stdout"), child.text()

    def stop(self, node: int) -> None:
        child = self.nodes.pop(node)
        child.kill()
        if child.text("stderr"):
            self.observations.append(child.text("stderr"))

    def close(self) -> None:
        for node in list(self.nodes):
            self.stop(node)

    def client_args(self, command: str, key: str, *args: str, timeout: int = 8000) -> list[str]:
        return [str(self.client), "--timeout-ms", str(timeout), "--shards", str(self.layout),
                *args, command, APP, key]

    def cli(self, command: str, key: str, *fields: str, request_id: uuid.UUID | None = None,
            timeout: int = 8000) -> tuple[int | None, str]:
        flags = ["--request-id", str(request_id)] if request_id is not None else []
        child = self.command([*self.client_args(command, key, *flags, timeout=timeout), *fields])
        status = child.finish(timeout / 1000 + 5)
        self.observations.append(f"{command} {key} exit={status}\n{child.text()}")
        return status, child.text("stdout")

    def event(self, node: int, event: uuid.UUID) -> list[tuple]:
        return rows(self.db(node), "SELECT sequence FROM delivery_log WHERE message_uuid=?", (event.bytes,))


def check_wrong_shard(deployment: Deployment) -> uuid.UUID:
    app, key = APP.encode(), FIRST_KEY.encode()
    rpc = uuid.uuid4()
    with closing(Conn("127.0.0.1", deployment.app_ports[3], 5)) as conn:
        for request in (
            get_request(rpc, app, key),
            put_request(rpc, app, key, b"wrong shard"),
            delete_request(rpc, app, key),
            incremental_request(rpc, app, key, uuid.uuid4(), ADD_ENTRY, b"wrong shard"),
            subscribe_request(rpc, app, key, resume_after=0),
            unsubscribe_request(rpc, app, key),
        ):
            kind, reply_id, fields = decode_body(conn.request(request))
            assert (kind, reply_id) == (WRONG_SHARD_RESPONSE, rpc), (kind, reply_id)
            assert int.from_bytes(fields.take(4), "big") == FIRST_SHARD
            assert fields.remaining() == 0

    child = deployment.command([str(deployment.client), "--timeout-ms", "4000",
                                str(deployment.app_ports[3]), "get", APP, FIRST_KEY])
    assert child.finish(6) == 1 and not child.text("stdout"), child.text()
    return rpc


def check_routing_and_failover(deployment: Deployment) -> None:
    for node in range(6):
        deployment.start(node)

    rejected_event = check_wrong_shard(deployment)
    first, second = uuid.uuid4(), uuid.uuid4()
    assert deployment.cli("put", FIRST_KEY, "first-shard", request_id=first) == (0, "OK\n")
    assert deployment.cli("put", SECOND_KEY, "second-shard", request_id=second) == (0, "OK\n")
    assert deployment.cli("get", FIRST_KEY) == (0, "first-shard\n")
    assert deployment.cli("get", SECOND_KEY) == (0, "second-shard\n")

    for node in range(6):
        expected, foreign = (first, second) if node < 3 else (second, first)
        eventually(lambda: deployment.event(node, expected), f"node {node} did not apply its event")
        assert deployment.event(node, foreign) == [], f"node {node} applied another shard's event"
        assert deployment.event(node, rejected_event) == [], f"node {node} applied a rejected write"

    watcher = deployment.command(deployment.client_args(
        "subscribe", FIRST_KEY, "--state", str(deployment.directory / "subscriber.db")))
    try:
        assert watcher.wait_for("first-shard", 10, "stdout"), watcher.text()
        assert watcher.wait_for(f"Connected to 127.0.0.1:{deployment.app_ports[0]}", 10, "stderr"), watcher.text()
        deployment.stop(0)
        assert deployment.cli("put", FIRST_KEY, "after-reconnect") == (0, "OK\n")
        assert watcher.wait_for("after-reconnect", 10, "stdout"), watcher.text()
        assert watcher.text("stdout").splitlines().count("first-shard") == 1, watcher.text()
        assert all(f"Connected to 127.0.0.1:{port}" not in watcher.text("stderr")
                   for port in deployment.app_ports[3:]), watcher.text()
    finally:
        watcher.kill()
        deployment.observations.append(watcher.text())

    deployment.stop(1)
    assert deployment.cli("get", FIRST_KEY, timeout=2000)[0] != 0
    assert deployment.cli("put", SECOND_KEY, "independent-quorum") == (0, "OK\n")
    assert deployment.cli("get", SECOND_KEY) == (0, "independent-quorum\n")


def check_database_identity(deployment: Deployment) -> None:
    # The local Raft membership stays identical while a different shard ID changes the hash layout.
    alternative = deployment.directory / "changed-layout.txt"
    alternative.write_text(deployment.layout_text(23), encoding="ascii")
    rejected = deployment.command(deployment.server_args(0, alternative))
    assert rejected.finish(5) == 1 and "Listening on" not in rejected.text("stdout"), rejected.text()
    deployment.observations.append(rejected.text())

    peers = ",".join(f"{replica + 1}@127.0.0.1:{deployment.raft_ports[replica]}" for replica in (1, 2))
    unsharded = deployment.command([
        str(deployment.server), str(deployment.app_ports[0]), str(deployment.db(0)), "--id", "1",
        "--raft-port", str(deployment.raft_ports[0]), "--peers", peers,
    ])
    assert unsharded.finish(5) == 1 and "Listening on" not in unsharded.text("stdout"), unsharded.text()
    deployment.observations.append(unsharded.text())

    equivalent = deployment.directory / "reordered-layout.txt"
    equivalent.write_text("\n".join(reversed(deployment.layout.read_text(encoding="ascii").splitlines())) + "\n",
                          encoding="ascii")
    deployment.start(0, equivalent)
    assert deployment.cli("get", FIRST_KEY) == (0, "after-reconnect\n")


def run_sharding(server: Path, client: Path, work: Path, env: dict[str, str]) -> list[dict]:
    deployment = Deployment(server, client, work, env)
    status, detail = "pass", "通过"
    try:
        check_routing_and_failover(deployment)
        check_database_identity(deployment)
    except Exception:
        status, detail = "fail", traceback.format_exc().strip()
    finally:
        deployment.close()
    return [{"name": "static_shard_routing", "status": status, "detail": detail,
             "commands": deployment.commands, "observations": deployment.observations}]
