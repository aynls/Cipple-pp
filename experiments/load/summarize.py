#!/usr/bin/env python3
"""Summarize Java vs C++ comparison run: join summaries + CPU/RSS csv.

Profiles:
  java            native RippleClient (embedded SQLite insert boundary)
  cpp_live        wire harness live (no cursor, no store; online-delivery)
  cpp_durable     wire harness durable-cursor-wire-only (NO store; NOT persistence-equivalent)
  cpp_persist     wire harness durable+persist-sqlite (per-sub SQLite commit; post-persistence)
Robustness: utf-8-sig everywhere (PowerShell Out-File BOM), int64 timestamp_ms,
skip malformed rows, single-sample dt=0, 'gone' procs reported separately.
"""

import argparse
import csv
import json
import os
import statistics


def load(p):
    if not p or not os.path.exists(p):
        return None
    try:
        with open(p, encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception as e:
        return {"_load_error": "%s: %s" % (p, e)}


def _to_int_ms(v):
    # timestamp_ms is int64 Unix ms (e.g. 1795...). Parse as int to avoid
    # float precision loss; accept float strings defensively.
    if v is None or (isinstance(v, str) and not v.strip()):
        raise ValueError("empty timestamp_ms")
    s = str(v).strip()
    try:
        return int(s)
    except ValueError:
        return int(float(s))


def cpu_stats(path, interval=None):
    if not path or not os.path.exists(path):
        return {"_note": "no cpu-rss.csv (sampler may not have started)"}
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                return {"_error": "empty csv (no header)"}
            # Normalize BOM/whitespace in header names.
            reader.fieldnames = [
                (h or "").strip().lstrip("\ufeff") for h in reader.fieldnames
            ]
            required = {"timestamp_ms", "pid", "name", "cpu_s", "rss_mb"}
            if not required.issubset(set(reader.fieldnames)):
                return {
                    "_error": "bad header: %s" % reader.fieldnames,
                    "samples_skipped": 0,
                }
            rows = list(reader)
    except Exception as e:
        return {"_error": "read failed: %s" % e}
    by = {}
    skipped = 0
    for r in rows:
        try:
            # Skip blank lines.
            if not r.get("timestamp_ms") and not r.get("pid"):
                skipped += 1
                continue
            t = _to_int_ms(r.get("timestamp_ms"))
            if interval is not None and not interval[0] <= t <= interval[1]:
                continue
            pid = str(r.get("pid", "")).strip()
            name = str(r.get("name", "")).strip() or "unknown"
            cpu = float(r.get("cpu_s"))
            rss = float(r.get("rss_mb"))
            if not pid:
                skipped += 1
                continue
            by.setdefault(pid + "|" + name, []).append((t, cpu, rss))
        except Exception:
            skipped += 1
            continue
    out = {}
    for k, v in by.items():
        try:
            v.sort(key=lambda x: x[0])
            ts = [x[0] for x in v]
            cpu = [x[1] for x in v]
            rss = [x[2] for x in v]
            dt = (ts[-1] - ts[0]) / 1000.0 if len(ts) > 1 else 0.0
            dcpu = cpu[-1] - cpu[0] if len(cpu) > 1 else 0.0
            is_gone = k.endswith("|gone")
            entry = {
                "samples": len(v),
                "rss_avg_mb": round(statistics.mean(rss), 2) if rss else 0.0,
                "rss_max_mb": round(max(rss), 2) if rss else 0.0,
                "cpu_s_delta": round(dcpu, 2),
                "cpu_pct_wall": round(dcpu / dt * 100, 2) if dt > 0 else 0.0,
                "t_first_ms": ts[0] if ts else -1,
                "t_last_ms": ts[-1] if ts else -1,
            }
            if is_gone:
                entry["note"] = (
                    "process exited during sampling (pid reused or stopped by runner)"
                )
            out[k] = entry
        except Exception as e:
            out[k] = {"error": str(e), "samples": len(v)}
    if skipped:
        out["_skipped_malformed_rows"] = skipped
    return out


def measurement_cpu(path, profile):
    if not profile:
        return {}
    start = profile.get("measurement_start_ms")
    end = profile.get("measurement_end_ms")
    if not start or not end:
        return {}
    return cpu_stats(path, (start, end))


def _delta(java, cpp):
    if not java or not cpp:
        return None
    if "_load_error" in java or "_load_error" in cpp:
        return {"_note": "missing summary (load error)"}
    try:

        def num(x):
            return x if isinstance(x, (int, float)) else None

        jl, cl = num(java.get("p50_ms")), num(cpp.get("p50_ms"))
        jp95, cp95 = num(java.get("p95_ms")), num(cpp.get("p95_ms"))
        jp99, cp99 = num(java.get("p99_ms")), num(cpp.get("p99_ms"))
        jg = num(java.get("goodput_ops")) or 0
        cg = num(cpp.get("goodput_ops")) or 0
        jc = num(java.get("coverage_pct")) or 0
        cc = num(cpp.get("coverage_pct")) or 0
        complete = jc == 100 and cc == 100 and not java.get("errors") and not cpp.get("errors")
        return {
            "complete_deliveries_in_both_trials": complete,
            "p50_cpp_minus_java_ms": (cl - jl)
            if complete and jl is not None and cl is not None and jl >= 0 and cl >= 0
            else None,
            "p95_cpp_minus_java_ms": (cp95 - jp95)
            if complete and jp95 is not None and cp95 is not None
            else None,
            "p99_cpp_minus_java_ms": (cp99 - jp99)
            if complete and jp99 is not None and cp99 is not None
            else None,
            "goodput_cpp_minus_java": round(cg - jg, 3),
            "coverage_cpp_minus_java_pp": round(cc - jc, 3),
        }
    except Exception as e:
        return {"_error": str(e)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--java", default=None)
    ap.add_argument("--cpp", default=None)
    ap.add_argument("--cpp-durable", default=None)
    ap.add_argument("--cpp-persist", default=None)
    ap.add_argument("--cpu", default=None)
    ap.add_argument("--out", required=True)
    ap.add_argument("--meta", default="{}")
    ap.add_argument("--meta-file", default=None)
    a = ap.parse_args()
    j = load(a.java)
    c = load(a.cpp)
    cd = load(a.cpp_durable)
    cp = load(a.cpp_persist)
    meta = {}
    try:
        if a.meta_file and os.path.exists(a.meta_file):
            with open(a.meta_file, encoding="utf-8-sig") as f:
                meta = json.load(f)
        else:
            meta = json.loads(a.meta) if a.meta else {}
    except Exception as e:
        meta = {"_meta_error": str(e), "meta_raw": a.meta, "meta_file": a.meta_file}
    comp = {
        "meta": meta,
        "java": j,
        "cpp_live": c,
        "cpp_durable": cd,
        "cpp_persist": cp,
        "cpu_rss": cpu_stats(a.cpu),
        "cpu_rss_measurement": {
            "java": measurement_cpu(a.cpu, j),
            "cpp_live": measurement_cpu(a.cpu, c),
            "cpp_persist": measurement_cpu(a.cpu, cp),
        },
        "profile_legend": {
            "java": "native RippleClient (embedded SQLite insert before SyncResponse)",
            "cpp_live": "wire harness live, no cursor, no store (online-delivery; comparable to Java online)",
            "cpp_durable": "wire harness durable-cursor-wire-only, NO store (NOT persistence-equivalent; do NOT delta vs Java)",
            "cpp_persist": "wire harness durable+persist-sqlite, per-sub INSERT OR IGNORE+commit before ACK (post-persistence; comparable to Java insert)",
        },
    }
    d_live = _delta(j, c)
    if d_live is not None:
        comp["delta_live_vs_java"] = d_live
        # Back-compat alias used by earlier docs.
        comp["delta"] = d_live
    d_persist = _delta(j, cp)
    if d_persist is not None:
        comp["delta_persist_vs_java"] = d_persist
    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(comp, f, indent=2)
    print(json.dumps(comp, indent=2)[:6000])


if __name__ == "__main__":
    main()
