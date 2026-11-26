"""Cipple++ frame codec.

Frame = int32 BE length (bytes after the length) + uint8 type + 16-byte UUID + fields.
Strings are uint64 BE length + raw bytes. Integers are big-endian.
"""

from __future__ import annotations

import socket
import struct
import uuid
from dataclasses import dataclass

MAX_FRAME = 16 * 1024 * 1024

HEARTBEAT_REQUEST = 0
HEARTBEAT_RESPONSE = 1
SYNC_REQUEST = 2
SYNC_RESPONSE = 3
DELETE_REQUEST = 4
DELETE_RESPONSE = 5
GET_REQUEST = 6
GET_RESPONSE = 7
PUT_REQUEST = 8
PUT_RESPONSE = 9
SUBSCRIBE_REQUEST = 10
SUBSCRIBE_RESPONSE = 11
UNSUBSCRIBE_REQUEST = 12
UNSUBSCRIBE_RESPONSE = 13
INCREMENTAL_REQUEST = 14
INCREMENTAL_RESPONSE = 15
DISPATCH_REQUEST = 16
DISPATCH_RESPONSE = 17
RETRY_RESPONSE = 18
HISTORY_LOST_RESPONSE = 19

OP_UPDATE = "update"
OP_DELETE = "delete"
OP_INCREMENTAL = "incremental_update"
ADD_ENTRY = "add_entry"
REMOVE_ENTRY = "remove_entry"


class ProtocolError(Exception):
    pass


def _u64(value: int) -> bytes:
    return struct.pack(">Q", value)


def _i32(value: int) -> bytes:
    return struct.pack(">i", value)


def _i64(value: int) -> bytes:
    return struct.pack(">q", value)


def _str(value: bytes) -> bytes:
    return _u64(len(value)) + value


def frame(msg_type: int, msg_id: uuid.UUID, fields: bytes = b"") -> bytes:
    body = bytes([msg_type]) + msg_id.bytes + fields
    if not 0 < len(body) <= MAX_FRAME:
        raise ProtocolError(f"frame length {len(body)} is outside the candidate limit")
    return struct.pack(">i", len(body)) + body


def encode_entity(
        msg_id: uuid.UUID,
        op: str,
        application: bytes,
        key: bytes,
        *,
        value: bytes | None = None,
        base: uuid.UUID | None = None,
        atomic: str | None = None,
        hlc: int = 0,
        server_id: int = 0,
) -> bytes:
    out = msg_id.bytes + _str(op.encode("ascii")) + _str(application) + _str(key)
    if op == OP_UPDATE:
        out += _str(value or b"")
    elif op == OP_INCREMENTAL:
        if base is None or atomic is None:
            raise ProtocolError("incremental entity requires a base and an atomic operation")
        out += base.bytes + _str(atomic.encode("ascii")) + _str(value or b"")
    elif op != OP_DELETE:
        raise ProtocolError(f"unknown operation {op}")
    return out + _i64(hlc) + _i32(server_id)


class Reader:
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def remaining(self) -> int:
        return len(self.data) - self.pos

    def take(self, size: int) -> bytes:
        if size < 0 or self.remaining() < size:
            raise ProtocolError("truncated field")
        chunk = self.data[self.pos: self.pos + size]
        self.pos += size
        return chunk

    def u8(self) -> int:
        return self.take(1)[0]

    def i32(self) -> int:
        return struct.unpack(">i", self.take(4))[0]

    def i64(self) -> int:
        return struct.unpack(">q", self.take(8))[0]

    def blob(self) -> bytes:
        length = struct.unpack(">Q", self.take(8))[0]
        if length > MAX_FRAME:
            raise ProtocolError("string length exceeds frame limit")
        return self.take(length)

    def text(self) -> str:
        return self.blob().decode("utf-8", "surrogateescape")

    def uid(self) -> uuid.UUID:
        return uuid.UUID(bytes=self.take(16))


@dataclass
class Entity:
    uuid: uuid.UUID
    op: str
    application: bytes
    key: bytes
    value: bytes | None
    base: uuid.UUID | None
    atomic: str | None
    hlc: int
    server_id: int


def decode_entity(reader: Reader) -> Entity:
    msg_id = reader.uid()
    op = reader.text()
    application = reader.blob()
    key = reader.blob()
    value = None
    base = None
    atomic = None
    if op == OP_UPDATE:
        value = reader.blob()
    elif op == OP_INCREMENTAL:
        base = reader.uid()
        atomic = reader.text()
        value = reader.blob()
    elif op != OP_DELETE:
        raise ProtocolError(f"unknown operation {op!r}")
    return Entity(msg_id, op, application, key, value, base, atomic, reader.i64(), reader.i32())


@dataclass
class Dispatch:
    uuid: uuid.UUID
    entity: Entity
    sequence: int
    base: Entity | None


def decode_body(body: bytes) -> tuple[int, uuid.UUID, Reader]:
    if len(body) < 17:
        raise ProtocolError("frame body is shorter than type and uuid")
    return body[0], uuid.UUID(bytes=body[1:17]), Reader(body[17:])


def put_request(msg_id: uuid.UUID, application: bytes, key: bytes, value: bytes) -> bytes:
    return frame(PUT_REQUEST, msg_id, _str(application) + _str(key) + _str(value))


def delete_request(msg_id: uuid.UUID, application: bytes, key: bytes) -> bytes:
    return frame(DELETE_REQUEST, msg_id, _str(application) + _str(key))


def get_request(msg_id: uuid.UUID, application: bytes, key: bytes) -> bytes:
    return frame(GET_REQUEST, msg_id, _str(application) + _str(key))


def subscribe_request(
        msg_id: uuid.UUID,
        application: bytes,
        key: bytes,
        *,
        resume_after: int | None = None,
) -> bytes:
    cursor = -1 if resume_after is None else resume_after
    return frame(SUBSCRIBE_REQUEST, msg_id, _str(application) + _str(key) + _i64(cursor))


def unsubscribe_request(msg_id: uuid.UUID, application: bytes, key: bytes) -> bytes:
    return frame(UNSUBSCRIBE_REQUEST, msg_id, _str(application) + _str(key))


def incremental_request(
        msg_id: uuid.UUID,
        application: bytes,
        key: bytes,
        base: uuid.UUID,
        atomic: str,
        value: bytes,
) -> bytes:
    fields = _str(application) + _str(key) + base.bytes + _str(atomic.encode("ascii")) + _str(value)
    return frame(INCREMENTAL_REQUEST, msg_id, fields)


def dispatch_response(msg_id: uuid.UUID, success: bool) -> bytes:
    return frame(DISPATCH_RESPONSE, msg_id, bytes([1 if success else 0]))


def sync_request(rpc_id: uuid.UUID, entity: bytes, from_id: int) -> bytes:
    return frame(SYNC_REQUEST, rpc_id, entity + _i32(from_id))


def decode_dispatch(body: bytes) -> Dispatch:
    msg_type, msg_id, reader = decode_body(body)
    if msg_type != DISPATCH_REQUEST:
        raise ProtocolError(f"expected dispatch, got type {msg_type}")
    entity = decode_entity(reader)
    sequence = reader.i64()
    base = decode_entity(reader) if entity.op == OP_INCREMENTAL else None
    if sequence < 0 or reader.remaining():
        raise ProtocolError("invalid dispatch sequence or trailing bytes")
    return Dispatch(msg_id, entity, sequence, base)


@dataclass
class GetReply:
    value: bytes
    items: list[Entity]

    @property
    def visible(self) -> bytes | None:
        if not self.items or self.items[-1].op == OP_DELETE:
            return None
        return self.value


def decode_get(body: bytes) -> GetReply:
    msg_type, _msg_id, reader = decode_body(body)
    if msg_type != GET_RESPONSE:
        raise ProtocolError(f"expected get response, got type {msg_type}")
    reader.blob()
    reader.blob()
    value = reader.blob()
    count = reader.i32()
    if count < 0:
        raise ProtocolError("negative get item count")
    items = [decode_entity(reader) for _ in range(count)]
    if reader.remaining():
        raise ProtocolError("trailing bytes in get response")
    return GetReply(value, items)


def decode_bool_response(body: bytes, expected: int) -> bool:
    msg_type, _msg_id, reader = decode_body(body)
    if msg_type != expected:
        raise ProtocolError(f"expected type {expected}, got {msg_type}")
    flag = reader.u8()
    if reader.remaining():
        raise ProtocolError("trailing bytes in bool response")
    return flag == 1


def decode_retry(body: bytes, request_id: uuid.UUID) -> None:
    msg_type, msg_id, reader = decode_body(body)
    if msg_type != RETRY_RESPONSE or msg_id != request_id or reader.remaining():
        raise ProtocolError(f"expected retry for {request_id}, got type {msg_type} for {msg_id}")


def sync_result(body: bytes) -> int | None:
    if not body or body[0] != SYNC_RESPONSE or len(body) < 18:
        return None
    return body[17]


class Conn:
    def __init__(self, host: str, port: int, timeout: float = 5.0):
        self.sock = socket.create_connection((host, port), timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""
        self.timeout = timeout

    def close(self) -> None:
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()

    def send(self, raw: bytes) -> None:
        self.sock.sendall(raw)

    def read_frame(self, timeout: float | None = None) -> bytes:
        self.sock.settimeout(self.timeout if timeout is None else timeout)
        while len(self.buf) < 4:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise EOFError("connection closed before a frame header")
            self.buf += chunk
        length = struct.unpack(">i", self.buf[:4])[0]
        if length <= 0 or length > MAX_FRAME:
            raise ProtocolError(f"invalid frame length {length}")
        need = 4 + length
        while len(self.buf) < need:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise EOFError("connection closed inside a frame")
            self.buf += chunk
        body = self.buf[4:need]
        self.buf = self.buf[need:]
        return body

    def request(self, raw: bytes, timeout: float | None = None) -> bytes:
        self.send(raw)
        return self.read_frame(timeout)
