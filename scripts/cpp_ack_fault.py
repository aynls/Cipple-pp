"""Replay the Java ACK control against two Cipple++ star replicas."""

import argparse
import contextlib
import json
from pathlib import Path
import subprocess
import tempfile
import threading
import time
import uuid

from demo import reserve_ports, stop, wait_listening
from java_ack_fault import Relay, query, wait_for


def exercise(server, client, directory):
    port_a, port_b = reserve_ports(2)
    db_a = directory / "node-a.db"
    db_b = directory / "node-b.db"
    identity = uuid.uuid4()
    marker = "sync-discarded-" + identity.hex

    def start(node, port, other_port, db):
        args = [
            str(server), str(port), str(db), "--id", str(node), "--overlay", "star",
            "--peers", f"{2 if node == 1 else 1}@127.0.0.1:{other_port}",
        ]
        output = stack.enter_context((directory / f"node-{node}.log").open("wb"))
        replica = subprocess.Popen(args, stdout=output, stderr=subprocess.STDOUT)
        stack.callback(stop, replica)
        wait_listening(port, replica)

    def put(key, value, request_id):
        subprocess.run(
            [str(client), "--request-id", str(request_id), str(port_a), "put", "ack-fault", key, value],
            capture_output=True, text=True, check=True, timeout=5,
        )

    def replica_count(entity_id):
        return query(db_b, "SELECT COUNT(*) FROM message WHERE uuid = ?", (entity_id.bytes,))[0]

    def source_ack():
        row = query(db_a, "SELECT acknowledged FROM ack WHERE node_id = 2 AND message_uuid = ?", (identity.bytes,))
        return None if row is None else row[0]

    with contextlib.ExitStack() as stack:
        relay = stack.enter_context(Relay(port_b))
        threading.Thread(target=relay.serve_forever, daemon=True).start()
        stack.callback(relay.finish)
        start(2, port_b, port_a, db_b)
        start(1, port_a, relay.server_address[1], db_a)
        probe = uuid.uuid4()
        put("probe", "ready", probe)
        wait_for(lambda: replica_count(probe), "the healthy relay carrying the probe")

        relay.block.set()
        put("key", marker, identity)
        wait_for(lambda: marker.encode() in relay.discarded, "the relay discarding SYNC")
        ack_before_heal = source_ack()
        count_before_heal = replica_count(identity)
        healed_at = time.monotonic()
        relay.block.clear()

        wait_for(lambda: source_ack() == 1, "a real durable replica confirmation", timeout=15)
        result = {
            "expected_recovery": ack_before_heal == 0 and count_before_heal == 0 and replica_count(identity) == 1,
            "write_response_success": True,
            "target_ack_before_heal": ack_before_heal,
            "target_ack_after_heal": source_ack(),
            "entity_uuid": str(identity),
            "discarded_bytes": len(relay.discarded),
            "peer_count_before_heal": count_before_heal,
            "peer_count_after_heal": replica_count(identity),
            "seconds_to_durable_confirmation_after_heal": round(time.monotonic() - healed_at, 3),
        }
        (directory / "result.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result, indent=2))
        return result["expected_recovery"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--client", type=Path, required=True)
    args = parser.parse_args()
    directory = Path(tempfile.mkdtemp(prefix="cipple-ack-")).resolve()
    print(f"Logs and databases: {directory}", flush=True)
    raise SystemExit(0 if exercise(args.server.resolve(), args.client.resolve(), directory) else 1)
