"""Localhost TCP proxies.

A proxy is the only fault injector. It can refuse connections, and it can drop
exactly one durability confirmation for a chosen entity before refusing. The
confirmation is a successful or duplicate SyncResponse whose
RPC id was observed on the SyncRequest carrying that entity.
"""

from __future__ import annotations

import socket
import struct
import threading
import uuid
from dataclasses import dataclass, field

from cipple_acceptance.protocol import MAX_FRAME, SYNC_REQUEST, SYNC_RESPONSE


@dataclass
class FrameNote:
    proxy: str
    direction: str
    msg_type: int
    rpc: str
    detail: str
    dropped: bool


@dataclass
class DropPlan:
    entity: uuid.UUID
    dropped: int = 0
    refusing: bool = False
    rpc_ids: set[uuid.UUID] = field(default_factory=set)
    notes: list[FrameNote] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)
    drop_first: bool = True

    def release(self) -> None:
        with self.lock:
            self.refusing = False

    def consider(self, proxy: str, direction: str, body: bytes) -> bool:
        """Return True when this frame must not be forwarded."""
        if not body:
            return False
        msg_type = body[0]
        rpc = uuid.UUID(bytes=body[1:17]) if len(body) >= 17 else None
        detail = ""
        drop = False
        with self.lock:
            if msg_type == SYNC_REQUEST and len(body) >= 33:
                entity = uuid.UUID(bytes=body[17:33])
                detail = f"entity={entity}"
                if entity == self.entity and rpc is not None:
                    self.rpc_ids.add(rpc)
            elif msg_type == SYNC_RESPONSE and len(body) >= 18 and rpc is not None:
                result = body[17]
                detail = f"result={result}"
                if self.drop_first and rpc in self.rpc_ids and result in (0,
                                                                          1) and self.dropped == 0 and not self.refusing:
                    drop = True
            if drop:
                self.dropped += 1
                self.refusing = True
            if msg_type in (SYNC_REQUEST, SYNC_RESPONSE) and len(self.notes) < 200:
                self.notes.append(
                    FrameNote(proxy, direction, msg_type, str(rpc) if rpc else "", detail, drop)
                )
        return drop


class Proxy:
    def __init__(self, name: str, port: int, upstream_port: int, plan: DropPlan | None = None):
        self.name = name
        self.port = port
        self.upstream_port = upstream_port
        self.plan = plan
        self.refusing = False
        self.accepts = 0
        self._lock = threading.Lock()
        self._sockets: list[socket.socket] = []
        self._stop = threading.Event()
        self._listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listen.bind(("127.0.0.1", port))
        self._listen.listen(64)
        self._listen.settimeout(0.2)
        self._thread = threading.Thread(target=self._accept_loop, name=f"proxy-{name}", daemon=True)
        self._thread.start()

    def set_upstream(self, port: int) -> None:
        with self._lock:
            self.upstream_port = port
        self._cut()

    def set_refusing(self, refusing: bool) -> None:
        self.refusing = refusing
        if refusing:
            self._cut()

    def snapshot_refusing(self) -> bool:
        planned = self.plan.refusing if self.plan is not None else False
        return self.refusing or planned

    def close(self) -> None:
        self._stop.set()
        self._cut()
        try:
            self._listen.close()
        except OSError:
            pass

    def _cut(self) -> None:
        with self._lock:
            sockets = list(self._sockets)
        for sock in sockets:
            _close(sock)

    def _track(self, sock: socket.socket) -> None:
        with self._lock:
            self._sockets.append(sock)

    def _untrack(self, sock: socket.socket) -> None:
        with self._lock:
            if sock in self._sockets:
                self._sockets.remove(sock)

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                client, _addr = self._listen.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            self.accepts += 1
            if self.snapshot_refusing():
                _close(client)
                continue
            upstream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                with self._lock:
                    target = self.upstream_port
                upstream.settimeout(5)
                upstream.connect(("127.0.0.1", target))
                upstream.settimeout(None)
            except OSError:
                _close(upstream)
                _close(client)
                continue
            client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            upstream.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._track(client)
            self._track(upstream)
            threading.Thread(
                target=self._pump, args=(client, upstream, "c2s"), daemon=True
            ).start()
            threading.Thread(
                target=self._pump, args=(upstream, client, "s2c"), daemon=True
            ).start()

    def _pump(self, src: socket.socket, dst: socket.socket, direction: str) -> None:
        buf = b""
        try:
            src.settimeout(0.5)
            while not self._stop.is_set():
                if self.snapshot_refusing():
                    break
                try:
                    chunk = src.recv(65536)
                except TimeoutError:
                    continue
                if not chunk:
                    break
                buf += chunk
                while len(buf) >= 4:
                    length = struct.unpack(">i", buf[:4])[0]
                    if length <= 0 or length > MAX_FRAME:
                        return
                    if len(buf) < 4 + length:
                        break
                    raw = buf[: 4 + length]
                    body = raw[4:]
                    buf = buf[4 + length:]
                    drop = self.plan.consider(self.name, direction, body) if self.plan else False
                    if drop or self.snapshot_refusing():
                        if drop:
                            self._cut()
                        return
                    dst.sendall(raw)
        except OSError:
            return
        finally:
            _close(src)
            _close(dst)
            self._untrack(src)
            self._untrack(dst)


def _close(sock: socket.socket) -> None:
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass
