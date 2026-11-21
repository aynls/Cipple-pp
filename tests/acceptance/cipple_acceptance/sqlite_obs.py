"""Read-only view of the candidate's contractual SQLite tables."""

from __future__ import annotations

import sqlite3
import time
import uuid
from pathlib import Path


def _uuid(blob: bytes | None) -> str:
    if isinstance(blob, bytes) and len(blob) == 16:
        return str(uuid.UUID(bytes=blob))
    if blob is None:
        return ""
    return blob.hex()


def inspect_db(path: Path) -> dict:
    uri = "file:" + path.resolve().as_posix() + "?mode=ro"
    last_error: Exception | None = None
    connection = None
    for _ in range(30):
        try:
            connection = sqlite3.connect(uri, uri=True, timeout=1)
            break
        except sqlite3.Error as error:
            last_error = error
            time.sleep(0.1)
    if connection is None:
        raise sqlite3.OperationalError(f"unable to open {path}: {last_error}")
    try:
        connection.execute("PRAGMA query_only = ON")
        messages = [
            {
                "uuid": _uuid(row[0]),
                "op": row[1],
                "hlc": row[2],
                "server_id": row[3],
                "atomic": row[4],
                "value": row[5],
                "base": _uuid(row[6]),
                "app": row[7],
                "key": row[8],
            }
            for row in connection.execute(
                """
                SELECT uuid,
                       operation_type,
                       last_update,
                       last_update_server_id,
                       atomic_operation,
                       value,
                       base_message_uuid,
                       application_name, key
                FROM message
                ORDER BY last_update, last_update_server_id, uuid
                """
            )
        ]
        acks = [
            {"uuid": _uuid(row[0]), "node_id": row[1], "acknowledged": row[2]}
            for row in connection.execute(
                "SELECT message_uuid, node_id, acknowledged FROM ack ORDER BY node_id"
            )
        ]
        journal = [
            {"sequence": row[0], "uuid": _uuid(row[1]), "app": row[2], "key": row[3]}
            for row in connection.execute(
                "SELECT sequence, message_uuid, application_name, key FROM delivery_log ORDER BY sequence"
            )
        ]
    finally:
        connection.close()
    return {"messages": messages, "acks": acks, "journal": journal}


def format_db(path: Path) -> str:
    try:
        data = inspect_db(path)
    except sqlite3.Error as error:
        return f"{path}: sqlite 读取失败: {error}"
    lines = [f"{path}"]
    for message in data["messages"]:
        rendered = dict(message)
        value = rendered["value"]
        rendered["value"] = value.decode("utf-8", "replace") if isinstance(value, bytes) else value
        lines.append(
            "  message uuid={uuid} op={op} atomic={atomic} hlc={hlc} server={server_id} "
            "base={base} value={value!r}".format(**rendered)
        )
    for ack in data["acks"]:
        lines.append(
            f"  ack uuid={ack['uuid']} node={ack['node_id']} acknowledged={ack['acknowledged']}"
        )
    for entry in data["journal"]:
        lines.append(f"  journal seq={entry['sequence']} uuid={entry['uuid']}")
    if len(lines) == 1:
        lines.append("  (no message, ack, or journal rows)")
    return "\n".join(lines)


def ack_bit(path: Path, message: uuid.UUID, node_id: int) -> int | None:
    data = inspect_db(path)
    target = str(message)
    for ack in data["acks"]:
        if ack["uuid"] == target and ack["node_id"] == node_id:
            return int(ack["acknowledged"])
    return None


def message_count(path: Path, message: uuid.UUID) -> int:
    target = str(message)
    return sum(1 for row in inspect_db(path)["messages"] if row["uuid"] == target)


def journal_has(path: Path, message: uuid.UUID) -> bool:
    target = str(message)
    return any(row["uuid"] == target for row in inspect_db(path)["journal"])


def message_hlc(path: Path, message: uuid.UUID) -> int | None:
    target = str(message)
    for row in inspect_db(path)["messages"]:
        if row["uuid"] == target:
            return int(row["hlc"])
    return None
