"""Process control for binaries this harness started."""

from __future__ import annotations

import os
import socket
import subprocess
import threading
import time
from pathlib import Path

def candidate_env(bin_path: Path | None = None) -> dict[str, str]:
    env = os.environ.copy()
    if bin_path is not None:
        env["PATH"] = str(bin_path) + os.pathsep + env.get("PATH", "")
    return env


def reserve_ports(count: int) -> list[int]:
    holders: list[socket.socket] = []
    ports: list[int] = []
    try:
        for _ in range(count):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("127.0.0.1", 0))
            holders.append(sock)
            ports.append(sock.getsockname()[1])
    finally:
        for sock in holders:
            sock.close()
    return ports


class Child:
    def __init__(self, args: list[str], cwd: Path, env: dict[str, str] | None = None):
        self.args = args
        self.proc = subprocess.Popen(
            args,
            cwd=str(cwd),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
        self.stdout = bytearray()
        self.stderr = bytearray()
        self._lock = threading.Lock()
        self._threads = [
            threading.Thread(target=self._drain, args=(self.proc.stdout, self.stdout), daemon=True),
            threading.Thread(target=self._drain, args=(self.proc.stderr, self.stderr), daemon=True),
        ]
        for thread in self._threads:
            thread.start()
        LIVE.append(self)

    def _drain(self, pipe, bucket: bytearray) -> None:
        # Buffered read(4096) waits until the pipe fills or the process exits.
        # A listening line is much shorter than that, so os.read returns the bytes already written.
        if pipe is None:
            return
        fd = pipe.fileno()
        while True:
            try:
                chunk = os.read(fd, 4096)
            except OSError:
                break
            if not chunk:
                break
            with self._lock:
                bucket.extend(chunk)

    def text(self, stream: str = "both") -> str:
        with self._lock:
            data = b""
            if stream in ("stdout", "both"):
                data += bytes(self.stdout)
            if stream in ("stderr", "both"):
                data += bytes(self.stderr)
        return data.decode("utf-8", "replace").replace("\r\n", "\n")

    def wait_for(self, needle: str, timeout: float, stream: str = "both") -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if needle in self.text(stream):
                return True
            if self.proc.poll() is not None:
                return needle in self.text(stream)
            time.sleep(0.05)
        return needle in self.text(stream)

    def wait(self, timeout: float) -> int | None:
        try:
            return self.proc.wait(timeout)
        except subprocess.TimeoutExpired:
            self.kill()
            return None

    def finish(self, timeout: float) -> int | None:
        code = self.wait(timeout)
        for thread in self._threads:
            thread.join(timeout=1)
        return code

    def kill(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                return
        for thread in self._threads:
            thread.join(timeout=1)


LIVE: list[Child] = []


def stop_all() -> None:
    for child in list(LIVE):
        child.kill()


def cmdline(args: list[str]) -> str:
    return subprocess.list2cmdline(args)
