"""Run the Cipple++ acceptance harness and write a Chinese report.

Process cases use the official CMake binaries given by --server and --client.
Run the harness after building the binaries.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
import time
import traceback
from pathlib import Path

ACCEPTANCE = Path(__file__).resolve().parent
ROOT = ACCEPTANCE.parents[1]
sys.path.insert(0, str(ACCEPTANCE))

from cipple_acceptance.raft import run_raft  # noqa: E402
from cipple_acceptance.runtime import candidate_env, stop_all  # noqa: E402
from cipple_acceptance.single_node import run_single_node  # noqa: E402


def _parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cipple++ 黑盒验收。使用已构建的 CMake 可执行文件，不重新编译源码。")
    parser.add_argument("--server", type=Path, help="cipple_server 可执行文件")
    parser.add_argument("--client", type=Path, help="cipple_client 可执行文件")
    parser.add_argument("--output", type=Path, help="报告和运行目录；省略时使用新建的系统临时目录")
    parser.add_argument("--bin-path", type=Path, help="前置到 PATH 的目录，供 sqlite3 等运行库使用")
    return parser.parse_args()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _case(name: str, status: str, detail: str, commands: list[str], observations: list[str]) -> dict:
    return {
        "name": name,
        "status": status,
        "detail": detail,
        "commands": commands,
        "observations": observations,
    }


def _render(report: dict) -> str:
    lines = [
        "Cipple++ 验收",
        f"时间 {report['started']}",
        f"仓库 {report['root']}",
        f"PATH 前置 {report['bin_path'] or '(未前置)'}",
        f"server {report['server']['path']} {report['server']['sha256']}",
        f"client {report['client']['path']} {report['client']['sha256']}",
        f"输出 {report['output']}",
        "",
    ]
    for case in report["cases"]:
        lines.append(f"[{case['status']}] {case['name']}")
        lines.append(case["detail"])
        for command in case["commands"]:
            lines.append("  命令 " + command)
        for observation in case["observations"]:
            lines.append(observation)
        lines.append("")
    counts: dict[str, int] = {}
    for case in report["cases"]:
        counts[case["status"]] = counts.get(case["status"], 0) + 1
    lines.append("合计 " + ", ".join(f"{key}={value}" for key, value in sorted(counts.items())))
    lines.append(f"退出码 {report['exit_code']}")
    lines.append(f"JSON {report['json_path']}")
    lines.append(f"TXT {report['text_path']}")
    lines.append(f"SHA {report['sha_path']}")
    return "\n".join(lines) + "\n"


def _append_exc(cases: list[dict], name: str, detail: str) -> None:
    cases.append(_case(name, "fail", detail, [], [traceback.format_exc()]))


def main() -> int:
    args = _parse()
    started = time.strftime("%Y-%m-%d %H:%M:%S")
    output = args.output.resolve() if args.output else Path(tempfile.mkdtemp(prefix="cipple-acceptance-"))
    output.mkdir(parents=True, exist_ok=True)
    work = output / "work"
    work.mkdir(parents=True, exist_ok=True)
    ucrt_root = os.environ.get("UCRT64_ROOT")
    bin_path = args.bin_path
    if bin_path is None and ucrt_root:
        bin_path = Path(ucrt_root) / "bin"
    if bin_path is not None:
        bin_path = bin_path.resolve()
    env = candidate_env(bin_path)
    server = args.server.resolve() if args.server else None
    client = args.client.resolve() if args.client else None
    json_path = output / "report.json"
    text_path = output / "report.txt"
    sha_path = output / "SHA256SUMS.txt"

    cases: list[dict] = []
    ready = server is not None and client is not None and server.is_file() and client.is_file()
    if ready:
        assert server is not None and client is not None
        runners = (
            ("single_node_runner", "单节点验收运行器自身失败。", run_single_node, work / "single-node"),
            ("raft_runner", "Raft 集群验收运行器自身失败。", run_raft, work / "raft"),
        )
        for name, detail, runner, folder in runners:
            try:
                cases.extend(runner(server, client, folder, env))
            except Exception:
                _append_exc(cases, name, detail)
    else:
        cases.append(
            _case(
                "candidate_binary",
                "blocked",
                "没有提供可用的 CMake 可执行文件，单节点和集群用例没有启动。",
                [],
                [f"server={server}", f"client={client}"],
            )
        )

    code = 0 if cases and all(case["status"] == "pass" for case in cases) else 1
    report = {
        "started": started,
        "root": str(ROOT),
        "bin_path": str(bin_path) if bin_path else "",
        "server": {
            "path": str(server) if server else "",
            "sha256": _sha256(server) if server is not None and server.is_file() else "",
        },
        "client": {
            "path": str(client) if client else "",
            "sha256": _sha256(client) if client is not None and client.is_file() else "",
        },
        "output": str(output),
        "work": str(work),
        "json_path": str(json_path),
        "text_path": str(text_path),
        "sha_path": str(sha_path),
        "exit_code": code,
        "cases": cases,
    }
    text = _render(report)
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    text_path.write_text(text, encoding="utf-8")
    entries = [
        (report["server"]["sha256"], report["server"]["path"]),
        (report["client"]["sha256"], report["client"]["path"]),
        (_sha256(json_path), str(json_path)),
        (_sha256(text_path), str(text_path)),
    ]
    sha_text = "".join(f"{digest}  {path}\n" for digest, path in entries if digest and path)
    sha_path.write_text(sha_text, encoding="utf-8")
    print(text, end="")
    print(sha_text, end="")
    return code


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        stop_all()
