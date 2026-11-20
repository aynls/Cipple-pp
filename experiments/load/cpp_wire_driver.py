#!/usr/bin/env python3
"""Faithful Python wire driver for Cipple++ comparison (no native client dependency).

Protocol (see common/tcp/frame_codec, buffer_writer/reader, message/*, entity/*):
  frame  = BE int32 body_len + body
  body   = u8 type + 16B uuid + fields
  string = BE u64 len + bytes ; bool = u8 (1/0) ; i32/i64 BE
  types: 10 PUT (app,key,value) / 11 PUT_RESP (uuid+bool)
         12 SUBSCRIBE (app,key,callback_str,i32 port[,i64 resume_after]) / 13 SUB_RESP
         18 DISPATCH (sig_str + EntityMessage + [i64 seq + [EntityMessage base]]) / 19 DISPATCH_RESP
  EntityMessage = 16B uuid + op_str + app + key + payload + i64 last_update + i32 server_id
    op update -> value str ; delete -> nothing ; incremental -> 16B base_uuid + atomic str + value str
  subscribe live (comparable online boundary): callback "" + i32 0, no resume_after.
  durable overhead mode: append i64 resume_after (e.g. 0).

Profiles (do NOT conflate): live (no cursor, no store) is the comparable
online-delivery boundary; durable-cursor-wire-only (--resume-after without
--persist-sqlite) is wire arrival only and NOT persistence-equivalent;
durable+persist-sqlite (--resume-after + --persist-sqlite DIR) does a real
per-subscriber SQLite INSERT OR IGNORE + commit BEFORE t_recv/ACK and is the
comparable post-persistence boundary (harness-side store, labelled as wire
harness vs native Java client; live profile kept distinct).

Observation boundary: t_publish immediately before sending PUT frame;
t_recv when a subscriber thread finishes parsing a DISPATCH frame whose entity
uuid matches a published request uuid (entity uuid == PUT request uuid server-side),
BEFORE sending back DISPATCH_RESP. This mirrors Java's post-insert hook
(after storage insert, before SyncResponse) at the corresponding online-delivery
boundary. Durable live-vs-cursor overhead is measured separately via --resume-after.
"""

import argparse, json, os, random, socket, sqlite3, struct, sys, threading, time, uuid

T_PUT_REQ, T_PUT_RESP = 10, 11
T_SUB_REQ, T_SUB_RESP = 12, 13
T_DISPATCH_REQ, T_DISPATCH_RESP = 18, 19


def u16():
    return uuid.uuid4().bytes


def enc_str(b: bytearray, s: str):
    d = s.encode()
    b += struct.pack(">Q", len(d)) + d


def dec_str(buf: bytes, pos: int):
    (n,) = struct.unpack_from(">Q", buf, pos)
    pos += 8
    s = buf[pos : pos + n].decode()
    return s, pos + n


def send_frame(sock: socket.socket, body: bytes):
    sock.sendall(struct.pack(">i", len(body)) + body)


def recv_exact(sock: socket.socket, n: int):
    out = b""
    while len(out) < n:
        ch = sock.recv(n - len(out))
        if not ch:
            raise ConnectionError("eof")
        out += ch
    return out


def recv_frame(sock: socket.socket):
    (ln,) = struct.unpack(">i", recv_exact(sock, 4))
    if ln <= 0:
        raise ValueError("bad frame len")
    return recv_exact(sock, ln)


def parse_entity(buf: bytes, pos: int):
    euuid = buf[pos : pos + 16]
    pos += 16
    op, pos = dec_str(buf, pos)
    app, pos = dec_str(buf, pos)
    key, pos = dec_str(buf, pos)
    val = None
    if op == "update":
        val, pos = dec_str(buf, pos)
    elif op == "incremental_update":
        base = buf[pos : pos + 16]
        pos += 16
        atomic, pos = dec_str(buf, pos)
        val, pos = dec_str(buf, pos)
    elif op == "delete":
        pass
    else:
        raise ValueError("unknown op " + op)
    (last,) = struct.unpack_from(">q", buf, pos)
    pos += 8
    (sid,) = struct.unpack_from(">i", buf, pos)
    pos += 4
    return {"uuid": euuid, "op": op, "app": app, "key": key, "value": val}, pos


def parse_dispatch(body: bytes):
    assert body[0] == T_DISPATCH_REQ
    reply_uuid = body[1:17]
    pos = 17
    sig, pos = dec_str(body, pos)
    ent, pos = parse_entity(body, pos)
    seq = None
    base = None
    if pos < len(body):
        (seq,) = struct.unpack_from(">q", body, pos)
        pos += 8
        if pos < len(body):
            base, pos = parse_entity(body, pos)
    return reply_uuid, sig, ent, seq, base


def put_encode(rid: bytes, app: str, key: str, value: str) -> bytes:
    b = bytearray()
    b.append(T_PUT_REQ)
    b += rid
    enc_str(b, app)
    enc_str(b, key)
    enc_str(b, value)
    return bytes(b)


def sub_encode(rid: bytes, app: str, key: str, resume_after=None) -> bytes:
    b = bytearray()
    b.append(T_SUB_REQ)
    b += rid
    enc_str(b, app)
    enc_str(b, key)
    enc_str(b, "")
    b += struct.pack(">i", 0)
    if resume_after is not None:
        b += struct.pack(">q", resume_after)
    return bytes(b)


def dispatch_resp_encode(reply_uuid: bytes) -> bytes:
    b = bytearray()
    b.append(T_DISPATCH_RESP)
    b += reply_uuid
    b.append(1)
    return bytes(b)


def parse_seq(value: str):
    try:
        return int(value.split(" ", 3)[1])
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--servers", required=True, help="host:port,host:port...")
    ap.add_argument("--app", required=True)
    ap.add_argument("--key", required=True)
    ap.add_argument("--subs", type=int, default=1)
    ap.add_argument("--warmup", type=int, default=5)
    ap.add_argument("--ops", type=int, default=20)
    ap.add_argument("--gap-ms", type=int, default=50)
    ap.add_argument("--deadline-ms", type=int, default=15000)
    ap.add_argument("--value-bytes", type=int, default=256)
    ap.add_argument("--t0", type=int, default=None)
    ap.add_argument(
        "--out", required=True, help="summary json path (raw .jsonl sibling)"
    )
    ap.add_argument(
        "--home",
        choices=["shared", "roundrobin"],
        default="roundrobin",
        help="legacy placement flag; deterministic index-mod is always used: "
        "sub i -> servers[i %% N], pub -> servers[0] (node 1). "
        "'shared' pins all to servers[0] (identical to index-mod when N=1).",
    )
    ap.add_argument(
        "--resume-after",
        type=int,
        default=None,
        help="set to e.g. 0 for durable-cursor mode (bare wire read; NOT persistence-equivalent)",
    )
    ap.add_argument(
        "--persist-sqlite",
        default=None,
        help="DIR for per-subscriber SQLite persistence (durable+persist profile). "
        "Each subscriber commits INSERT OR IGNORE on msg_uuid before t_recv/ACK. "
        "Requires --resume-after for the comparable post-persistence boundary.",
    )
    ap.add_argument("--timeout", type=int, default=30)
    a = ap.parse_args()
    servers = []
    for s in a.servers.split(","):
        h, p = s.rsplit(":", 1)
        servers.append((h, int(p)))
    t0 = a.t0 if a.t0 is not None else int(time.time() * 1000)
    # Deterministic placement by node index (matches Java LoadDriver):
    # sub i -> servers[i % N], pub -> servers[0] (node 1).
    # 'shared' is the degenerate N=1 case (all on servers[0]); otherwise index-mod.
    sub_homes = []
    for i in range(a.subs):
        if a.home == "shared":
            sub_homes.append(servers[0])
        else:
            sub_homes.append(servers[i % len(servers)])
    pub_home = servers[0]
    placement = "index-mod: sub i -> servers[i % N], pub -> servers[0] (node 1)"
    persist_dir = a.persist_sqlite
    if persist_dir is not None and a.resume_after is None:
        print(
            "WARNING: --persist-sqlite without --resume-after: "
            "persist store without durable cursor; labelling as harness-persist (no cursor).",
            file=sys.stderr,
        )
    persist_conns = {}
    if persist_dir is not None:
        os.makedirs(persist_dir, exist_ok=True)
        for i in range(a.subs):
            dbp = os.path.join(persist_dir, f"cpp-sub{i}.db")
            # Each DB is used by exactly one subscriber thread; allow cross-thread
            # creation (main thread creates, worker thread uses).
            con = sqlite3.connect(
                dbp, timeout=30.0, isolation_level=None, check_same_thread=False
            )
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=FULL")
            con.execute(
                "CREATE TABLE IF NOT EXISTS deliveries("
                "msg_uuid TEXT PRIMARY KEY, app TEXT, key TEXT, "
                "value TEXT, seq INTEGER, req_uuid TEXT, t_commit_ms INTEGER)"
            )
            persist_conns[i] = con

    lock = threading.Lock()
    recv = {}  # (seq, sub) -> dict
    errors = []
    stop = threading.Event()
    ready = set()
    put_replies = {}

    def subscriber_loop(idx, host, port):
        try:
            sk = socket.create_connection((host, port), timeout=a.timeout)
            sk.settimeout(a.timeout)
            rid = u16()
            send_frame(sk, sub_encode(rid, a.app, a.key, a.resume_after))
            body = recv_frame(sk)
            if not (body[0] == T_SUB_RESP and body[1:17] == rid and body[17] == 1):
                with lock:
                    errors.append(f"sub{idx}: subscribe rejected")
                return
            with lock:
                ready.add(idx)
            while not stop.is_set():
                try:
                    sk.settimeout(0.2)
                    body = recv_frame(sk)
                except socket.timeout:
                    continue
                except Exception as e:
                    with lock:
                        errors.append(f"sub{idx}: conn closed {e}")
                    return
                if body[0] != T_DISPATCH_REQ:
                    continue
                try:
                    reply_uuid, sig, ent, seq, base = parse_dispatch(body)
                except Exception as e:
                    with lock:
                        errors.append(f"sub{idx}: dispatch parse {e}")
                    continue
                # Persist profile: real per-subscriber SQLite transaction BEFORE
                # t_recv/ACK (mirrors Java post-insert hook). Insert-once on
                # msg_uuid; t_recv is after commit.
                t_recv = int(time.time() * 1000)
                persist_err = ""
                if idx in persist_conns:
                    try:
                        con = persist_conns[idx]
                        msg_hex = ent["uuid"].hex()
                        now_ms = int(time.time() * 1000)
                        con.execute("BEGIN IMMEDIATE")
                        try:
                            con.execute(
                                "INSERT OR IGNORE INTO deliveries"
                                "(msg_uuid, app, key, value, seq, req_uuid, t_commit_ms)"
                                " VALUES (?,?,?,?,?,?,?)",
                                (
                                    msg_hex,
                                    ent["app"],
                                    ent["key"],
                                    ent.get("value") or "",
                                    parse_seq(ent["value"])
                                    if ent.get("value")
                                    else None,
                                    "",
                                    now_ms,
                                ),
                            )
                            con.execute("COMMIT")
                        except Exception:
                            try:
                                con.execute("ROLLBACK")
                            except Exception:
                                pass
                            raise
                        t_recv = int(time.time() * 1000)
                    except Exception as e:
                        persist_err = f"sub{idx}: sqlite persist {e}"
                        with lock:
                            errors.append(persist_err)
                        sk.close()
                        return
                t_recv_ns = time.perf_counter_ns()
                # ACK after parse (live) or after commit (persist).
                try:
                    send_frame(sk, dispatch_resp_encode(reply_uuid))
                except Exception:
                    pass
                if ent["app"] == a.app and ent["key"] == a.key and ent["value"]:
                    s = parse_seq(ent["value"])
                    if s is not None:
                        with lock:
                            if (s, idx) not in recv:
                                recv[(s, idx)] = {
                                    "msg_uuid": ent["uuid"].hex(),
                                    "t_recv": t_recv,
                                    "t_recv_ns": t_recv_ns,
                                    "seq_server": seq,
                                }
        except Exception as e:
            with lock:
                errors.append(f"sub{idx}: {e}")

    threads = [
        threading.Thread(target=subscriber_loop, args=(i, h, p), daemon=True)
        for i, (h, p) in enumerate(sub_homes)
    ]
    for t in threads:
        t.start()
    ready_until = time.monotonic() + a.timeout
    while time.monotonic() < ready_until:
        with lock:
            if len(ready) == a.subs or errors:
                break
        time.sleep(0.01)
    if len(ready) != a.subs:
        errors.append(f"subscribe barrier: {len(ready)}/{a.subs} ready")
    time.sleep(2.0)
    t_pub = {}
    t_pub_ns = {}
    warmup_ids = []
    request_ids = []
    publisher = socket.create_connection(pub_home, timeout=a.timeout)
    publisher.settimeout(None)

    def read_put_replies():
        try:
            while not stop.is_set():
                response = recv_frame(publisher)
                with lock:
                    if len(response) != 18 or response[0] != T_PUT_RESP:
                        errors.append("invalid PUT response")
                    else:
                        put_replies[response[1:17]] = response[17] == 1
        except Exception as error:
            if not stop.is_set():
                with lock:
                    errors.append(f"publisher: {error}")

    def publish(sequence, request_id, measured=False):
        now_ms = int(time.time() * 1000)
        prefix = f"{t0} {sequence} {now_ms} "
        value = prefix + "x" * max(0, a.value_bytes - len(prefix.encode()))
        frame = put_encode(request_id, a.app, a.key, value)
        if measured:
            t_pub[sequence] = now_ms
            t_pub_ns[sequence] = time.perf_counter_ns()
        send_frame(publisher, frame)

    reply_reader = threading.Thread(target=read_put_replies, daemon=True)
    reply_reader.start()
    try:
        # warmup
        for w in range(a.warmup):
            seq = 1000000 + w
            rid = u16()
            warmup_ids.append(rid)
            publish(seq, rid)
            if a.gap_ms > 0:
                time.sleep(a.gap_ms / 1000.0)
            # best-effort drain for warmup
            end = time.monotonic() + min(a.deadline_ms / 1000, 5.0)
            while time.monotonic() < end:
                with lock:
                    ok = all((seq, i) in recv for i in range(a.subs))
                if ok:
                    break
                time.sleep(0.01)
        # measurement
        wall_start = int(time.time() * 1000)
        schedule = time.monotonic()
        for seq in range(a.ops):
            remaining = schedule + seq * a.gap_ms / 1000 - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)
            rid = u16()
            request_ids.append(rid)
            with lock:
                recv.setdefault(("req", seq), rid.hex())
            publish(seq, rid, measured=True)
        last_pub = t_pub.get(a.ops - 1, 0)
        # drain
        deadline = time.monotonic() + a.deadline_ms / 1000.0
        drained = False
        while time.monotonic() < deadline:
            with lock:
                ok = all((s, i) in recv for s in range(a.ops) for i in range(a.subs))
                ok = ok and all(rid in put_replies for rid in request_ids)
            if ok:
                drained = True
                break
            time.sleep(0.01)
        wall_end = int(time.time() * 1000)
        if not drained:
            with lock:
                errors.append(f"drain_timeout within {a.deadline_ms}ms")
        with lock:
            accepted = sum(put_replies.get(rid, False) for rid in request_ids)
            if accepted != a.ops:
                errors.append(f"publisher accepted {accepted}/{a.ops}")
            warmup_accepted = sum(put_replies.get(rid, False) for rid in warmup_ids)
            if warmup_accepted != a.warmup:
                errors.append(f"warm-up accepted {warmup_accepted}/{a.warmup}")
    finally:
        stop.set()
        try:
            publisher.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        publisher.close()
        reply_reader.join(timeout=5)
    for t in threads:
        t.join(timeout=5)
    for con in persist_conns.values():
        try:
            con.close()
        except Exception:
            pass

    raw_path = a.out[:-5] + ".jsonl" if a.out.endswith(".json") else a.out + ".jsonl"
    os.makedirs(os.path.dirname(os.path.abspath(raw_path)), exist_ok=True)
    lats = []
    per = [[] for _ in range(a.subs)]
    received = 0
    with open(raw_path, "w", encoding="utf-8") as f:
        for seq in range(a.ops):
            tp = t_pub.get(seq, -1)
            for i in range(a.subs):
                r = recv.get((seq, i))
                if r and tp != -1:
                    lat = round((r["t_recv_ns"] - t_pub_ns[seq]) / 1_000_000, 3)
                    lats.append(lat)
                    per[i].append(lat)
                    received += 1
                    f.write(
                        json.dumps(
                            {
                                "seq": seq,
                                "subscriber": i,
                                "msg_uuid": r["msg_uuid"],
                                "req_uuid": recv.get(("req", seq), ""),
                                "t_publish_ms": tp,
                                "t_recv_ms": r["t_recv"],
                                "latency_ms": lat,
                                "result": "ok",
                                "error": "",
                                "persist": "sqlite-commit"
                                if persist_dir is not None
                                else "none",
                                "cursor": ("durable-%s" % a.resume_after)
                                if a.resume_after is not None
                                else "live",
                            }
                        )
                        + "\n"
                    )
                else:
                    f.write(
                        json.dumps(
                            {
                                "seq": seq,
                                "subscriber": i,
                                "msg_uuid": "",
                                "req_uuid": recv.get(("req", seq), ""),
                                "t_publish_ms": tp,
                                "t_recv_ms": -1,
                                "latency_ms": -1,
                                "result": "missing",
                                "error": "not_received_within_deadline",
                            }
                        )
                        + "\n"
                    )

    def pct(v, q):
        if not v:
            return -1
        v = sorted(v)
        import math

        idx = max(0, min(len(v) - 1, math.ceil(q * len(v)) - 1))
        return v[idx]

    total = a.ops * a.subs
    comp = sum(1 for s in range(a.ops) if all((s, i) in recv for i in range(a.subs)))
    wall_s = max(1, wall_end - wall_start) / 1000.0
    last_recv = 0
    with lock:
        for (k, i), r in list(recv.items()):
            if isinstance(k, int) and k < a.ops and r.get("t_recv", 0) > last_recv:
                last_recv = r["t_recv"]
    if persist_dir is not None and a.resume_after is not None:
        engine = "cpp-wire-python+persist-sqlite"
        observation = (
            "t_publish=before PUT frame send; t_recv=after per-subscriber SQLite "
            "INSERT OR IGNORE + commit, before/with ACK send "
            "(durable cursor + harness-side persistence; wire harness, NOT native C++ client). "
            "Comparable post-persistence boundary vs Java native insert. Bare cursor read "
            "without sqlite is NOT equivalent and is labelled separately."
        )
    elif persist_dir is not None:
        engine = "cpp-wire-python+persist-sqlite(no-cursor)"
        observation = (
            "t_publish=before PUT frame send; t_recv=after per-subscriber SQLite "
            "INSERT OR IGNORE + commit, before/with ACK send "
            "(harness-side persistence without durable cursor; wire harness, NOT native client)."
        )
    elif a.resume_after is not None:
        engine = "cpp-wire-python(durable-cursor-wire-only)"
        observation = (
            "t_publish=before PUT frame send; t_recv=after DISPATCH parse, before/with ACK send "
            "(durable cursor wire read only, NO store; NOT persistence-equivalent; "
            "do NOT compare against Java native insert)."
        )
    else:
        engine = "cpp-wire-python(live)"
        observation = (
            "t_publish=before PUT frame send; t_recv=after DISPATCH parse, before/with ACK send "
            "(live, no cursor, no store; wire harness, NOT native C++ client). "
            "Online-delivery boundary comparable to Java native online delivery."
        )
    summary = {
        "engine": engine,
        "observation": observation,
        "placement": placement,
        "persist_sqlite_dir": persist_dir,
        "servers": a.servers,
        "sub_homes": [f"{h}:{p}" for h, p in sub_homes],
        "pub_home": f"{pub_home[0]}:{pub_home[1]}",
        "app": a.app,
        "key": a.key,
        "num_subscribers": a.subs,
        "warmup_ops": a.warmup,
        "meas_ops": a.ops,
        "gap_ms": a.gap_ms,
        "deadline_ms": a.deadline_ms,
        "value_bytes": a.value_bytes,
        "t0_ms": t0,
        "resume_after": a.resume_after,
        "subscribe_settle_ms": 2000,
        "subscribed": len(ready),
        "measurement_start_ms": wall_start,
        "measurement_end_ms": wall_end,
        "send_span_ms": (max(t_pub_ns.values()) - min(t_pub_ns.values())) / 1_000_000 if t_pub_ns else 0,
        "wall_ms": wall_end - wall_start,
        "completed_ops": comp,
        "goodput_ops": round(comp / wall_s, 3),
        "coverage_pct": round(received * 100.0 / total if total else 0, 3),
        "received_pairs": received,
        "total_pairs": total,
        "drain_ms": (last_recv - last_pub) if last_recv and last_pub else -1,
        "p50_ms": pct(lats, 0.5),
        "p95_ms": pct(lats, 0.95),
        "p99_ms": pct(lats, 0.99),
        "per_subscriber": [
            {
                "subscriber": i,
                "n": len(per[i]),
                "p50_ms": pct(per[i], 0.5),
                "p95_ms": pct(per[i], 0.95),
                "p99_ms": pct(per[i], 0.99),
            }
            for i in range(a.subs)
        ],
        "errors": errors,
        "raw_jsonl": raw_path,
    }
    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))
    return 0 if received == total and not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
