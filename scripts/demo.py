"""Run a three-replica demo using the installed server and client binaries."""

import argparse
import contextlib
from pathlib import Path
import queue
import socket
import subprocess
import tempfile
import threading
import time


def reserve_ports(count):
    with contextlib.ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in range(count)]
        for listener in sockets:
            listener.bind(("127.0.0.1", 0))
        return [listener.getsockname()[1] for listener in sockets]


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def wait_listening(port, process):
    deadline = time.monotonic() + 10
    while process.poll() is None and time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"Replica at {port} did not start (exit={process.poll()})")


class Watcher:
    def __init__(self, stack, client, port, state, log):
        stderr = stack.enter_context(log.open("w", encoding="utf-8"))
        self.process = subprocess.Popen(
            [str(client), "--state", str(state), str(port), "subscribe", "demo", "feature"],
            stdout=subprocess.PIPE, stderr=stderr, text=True, encoding="utf-8",
        )
        stack.callback(stop, self.process)
        self.lines = queue.Queue()
        threading.Thread(target=self.read_output, daemon=True).start()

    def read_output(self):
        with self.process.stdout as output:
            for line in output:
                self.lines.put(line.rstrip("\r\n"))
        self.lines.put(None)

    def expect(self, expected):
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            line = self.lines.get(timeout=max(0.001, deadline - time.monotonic()))
            if line == expected:
                return
            if line is None:
                break
        raise RuntimeError(f"Subscriber did not report {expected!r}")


def run_demo(server, client, directory):
    ports = reserve_ports(6)
    application_ports, raft_ports = ports[:3], ports[3:]
    state = directory / "subscriber.db"

    def run_client(index, command, *args):
        ordered = [application_ports[index]] + [port for port in application_ports if port != application_ports[index]]
        endpoints = ",".join(f"127.0.0.1:{port}" for port in ordered)
        return subprocess.run(
            [str(client), "--timeout-ms", "7000", endpoints, command, "demo", "feature", *args],
            capture_output=True, text=True, encoding="utf-8", timeout=9,
        )

    def checked(index, command, *args):
        result = run_client(index, command, *args)
        if result.returncode:
            raise RuntimeError(f"{command} (replica {index + 1}): {result.stderr.strip()}")
        return result.stdout.strip()

    with contextlib.ExitStack() as stack:
        cluster = []
        for index, port in enumerate(application_ports):
            log = stack.enter_context((directory / f"replica-{index + 1}.log").open("w", encoding="utf-8"))
            peers = ",".join(f"{peer + 1}@127.0.0.1:{other}" for peer, other in enumerate(raft_ports) if peer != index)
            replica = subprocess.Popen(
                [str(server), str(port), str(directory / f"replica-{index + 1}.db"),
                 "--id", str(index + 1), "--raft-port", str(raft_ports[index]), "--peers", peers],
                stdout=log, stderr=subprocess.STDOUT,
            )
            stack.callback(stop, replica)
            cluster.append(replica)
        for port, replica in zip(application_ports, cluster):
            wait_listening(port, replica)
        print(f"Started 3 replicas on {application_ports}")

        subscriber = Watcher(stack, client, application_ports[1], state, directory / "subscriber-first.log")
        subscriber.expect("Subscribed")
        checked(0, "put", "mode=canary")
        subscriber.expect("mode=canary")
        stop(subscriber.process)
        print("Replica 2 delivered the initial write. Subscriber is now offline.")

        base = checked(0, "get-base")
        checked(0, "incremental", base, "add_entry", ";region=local")
        if checked(2, "get") != "mode=canary;region=local":
            raise RuntimeError("The committed increment was not readable")
        checked(2, "delete")
        deleted = run_client(0, "get")
        if deleted.returncode != 1 or "Configuration not found" not in deleted.stderr:
            raise RuntimeError("GET did not observe the committed deletion")

        resumed = Watcher(stack, client, application_ports[2], state, directory / "subscriber-resumed.log")
        resumed.expect("Subscribed")
        resumed.expect("mode=canary;region=local")
        resumed.expect("<deleted>")
        print("The same durable subscription caught up at replica 3.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True, help="Path to cipple_server")
    parser.add_argument("--client", type=Path, required=True, help="Path to cipple_client")
    parser.add_argument("--output", type=Path, help="Parent directory for databases and logs")
    args = parser.parse_args()
    if args.output:
        args.output.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix="cipple-demo-", dir=args.output)).resolve()
    print(f"Logs and databases: {directory}", flush=True)
    run_demo(args.server.resolve(), args.client.resolve(), directory)
    print("Demo passed")


if __name__ == "__main__":
    main()
