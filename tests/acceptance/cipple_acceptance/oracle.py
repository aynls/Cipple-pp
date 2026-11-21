"""Compute expected configuration state for the acceptance cases.

Order entities by (HLC, server id, UUID). Deletes hide the visible value.
Apply increments to their referenced full values and apply each UUID only once.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass


@dataclass(frozen=True)
class Version:
    uuid: uuid.UUID
    op: str
    value: str | None = None
    base: uuid.UUID | None = None
    atomic: str | None = None
    hlc: int = 0
    server_id: int = 0


def apply_increment(base: str, atomic: str, fragment: str) -> str:
    if atomic == "add_entry":
        return base + fragment
    if fragment == "":
        return base
    return base.replace(fragment, "")


def resolve(history: list[Version]) -> str | None:
    if not history:
        return None
    winner = max(history, key=lambda item: (item.hlc, item.server_id, item.uuid.bytes))
    if winner.op == "delete":
        return None
    if winner.op == "update":
        return winner.value or ""
    if winner.op != "incremental_update" or winner.base is None or winner.atomic is None:
        raise AssertionError("incremental winner has no base")
    base = next((item for item in history if item.uuid == winner.base and item.op == "update"), None)
    if base is None or base.value is None:
        raise AssertionError("incremental winner has no full update base")
    return apply_increment(base.value, winner.atomic, winner.value or "")


def absorb(history: list[Version], version: Version) -> list[Version]:
    if any(item.uuid == version.uuid for item in history):
        return list(history)
    return [*history, version]


def check_oracle() -> list[str]:
    notes: list[str] = []
    base_id = uuid.UUID("00000000-0000-4000-8000-000000000001")
    inc_id = uuid.UUID("00000000-0000-4000-8000-000000000002")
    delete_id = uuid.UUID("00000000-0000-4000-8000-000000000003")
    skew_id = uuid.UUID("00000000-0000-4000-8000-000000000004")
    base = Version(base_id, "update", "base", hlc=1000, server_id=1)
    incremental = Version(
        inc_id, "incremental_update", "+inc", base=base_id, atomic="add_entry", hlc=2000, server_id=1
    )
    deletion = Version(delete_id, "delete", hlc=3000, server_id=1)
    skew = Version(skew_id, "update", "skew", hlc=500, server_id=3)

    replaced = apply_increment("a-x-x-b", "remove_entry", "-x")
    if replaced != "a-b":
        raise AssertionError(f"remove_entry produced {replaced!r}")
    if apply_increment("a-x-x-b", "remove_entry", "") != "a-x-x-b":
        raise AssertionError("empty remove_entry changed the base")
    notes.append("remove_entry 删除全部字面匹配，空片段不改变原文")

    once = absorb(absorb([base], incremental), incremental)
    if len(once) != 2 or resolve(once) != "base+inc":
        raise AssertionError("duplicate incremental was applied twice")
    naive = base.value + incremental.value + incremental.value
    if naive == resolve(once):
        raise AssertionError("oracle collapsed the double-apply distinction")
    notes.append("同一增量 UUID 第二次吸收被忽略；可见值保持 base+inc，而不是 base+inc+inc")

    masked = absorb(once, deletion)
    if resolve(masked) is not None:
        raise AssertionError("delete did not win")
    if not any(item.uuid == inc_id for item in masked):
        raise AssertionError("delete dropped the incremental from history")
    notes.append("删除赢得可见值，但增量仍留在日志里")

    skewed = absorb(masked, skew)
    if resolve(skewed) is not None:
        raise AssertionError("lower HLC replaced the delete")
    if max(skewed, key=lambda item: (item.hlc, item.server_id, item.uuid.bytes)).uuid != delete_id:
        raise AssertionError("winner was not the delete")
    tied_low = Version(uuid.UUID(int=1), "update", "low", hlc=3000, server_id=1)
    tied_high = Version(uuid.UUID(int=2), "update", "high", hlc=3000, server_id=2)
    if resolve([tied_low, tied_high]) != "high":
        raise AssertionError("equal HLC did not use server id")
    notes.append("排序键是 (HLC, server id, UUID)；更低的 HLC 不能把已删除的值写回来")
    return notes
