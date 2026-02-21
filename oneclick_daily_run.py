from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


BASE_DIR = Path(__file__).resolve().parent
LOG_DIR_DEFAULT = BASE_DIR / "logs"


# ----------------------------
# utils
# ----------------------------
def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def mk_run_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _atomic_write_text(path: Path, text: str, retries: int = 3) -> None:
    """
    Atomic write with minimal retry to survive transient locks.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    last: Optional[Exception] = None
    for i in range(retries):
        try:
            tmp.write_text(text, encoding="utf-8")
            os.replace(str(tmp), str(path))
            return
        except PermissionError as e:
            last = e
            time.sleep([0.2, 0.5, 1.0][min(i, 2)])
        except Exception as e:
            last = e
            time.sleep(0.2)
    if last:
        raise last


def _write_status_files(status_dir: Path, payload: Dict[str, Any]) -> None:
    status_dir.mkdir(parents=True, exist_ok=True)
    js = status_dir / "RUN_STATUS.json"
    txt = status_dir / "RUN_STATUS.txt"

    _atomic_write_text(js, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    _atomic_write_text(txt, render_run_status_txt(payload))


def render_run_status_txt(payload: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"updated_at: {payload.get('updated_at','')}")
    lines.append(f"run_id: {payload.get('run_id','')}")
    lines.append(f"overall_status: {payload.get('overall_status','')}")
    lines.append("")

    for k in ("mode", "input", "output", "top20", "log_file"):
        if k in payload:
            lines.append(f"{k}: {payload[k]}")
    lines.append("")

    if "primary" in payload:
        p = payload["primary"]
        lines.append("[primary]")
        for kk in ("script", "rc", "error"):
            if kk in p and p[kk] not in (None, "", []):
                lines.append(f"  {kk}: {p[kk]}")
        lines.append("")
    if "fallback" in payload:
        f = payload["fallback"]
        lines.append("[fallback]")
        for kk in ("script", "rc", "error"):
            if kk in f and f[kk] not in (None, "", []):
                lines.append(f"  {kk}: {f[kk]}")
        lines.append("")
    if "notes" in payload:
        lines.append("notes:")
        lines.append(str(payload["notes"]))
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _read_csv_header(path: Path) -> List[str]:
    """
    Read only header row. Handle UTF-8 BOM (\\ufeff).
    """
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, [])
    return header


def _assert_output_schema(out_csv: Path, required: List[str]) -> Tuple[bool, List[str]]:
    if not out_csv.exists() or out_csv.stat().st_size <= 0:
        return False, ["file_missing_or_empty"]

    header = _read_csv_header(out_csv)
    # normalize BOM in first column if any
    header_norm = [h.lstrip("\ufeff") for h in header]
    missing = [c for c in required if c not in header_norm]
    if missing:
        return False, missing
    return True, []


def _run_subprocess(cmd: List[str], log_path: Path, env: Dict[str, str]) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as logf:
        logf.write(f"\n===== {now_ts()} RUN =====\n")
        logf.write("CMD: " + " ".join(cmd) + "\n")
        logf.flush()
        p = subprocess.run(cmd, stdout=logf, stderr=logf, env=env, cwd=str(BASE_DIR))
        logf.write(f"RC: {p.returncode}\n")
        logf.flush()
        return p.returncode


@dataclass
class RunResult:
    ok: bool
    rc: int
    script: str
    error: str = ""


# ----------------------------
# core logic
# ----------------------------
def run_oneclick(
    mode: str,
    in_path: Path,
    out_path: Path,
    top20_path: Path,
    python_exe: str,
    primary_script: Path,
    fallback_script: Path,
    log_dir: Path,
    run_id: str,
) -> Tuple[RunResult, Optional[RunResult], Path, Path]:
    """
    Returns: (primary_result, fallback_result_or_none, final_out_path, final_top20_path)
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"oneclick_{mode}_{in_path.stem}_{run_id}.log"

    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    env["RUN_ID"] = run_id  # optional for downstream scripts

    required_cols = ["code"]  # minimal: you can extend if needed

    def build_cmd(script: Path, outp: Path, top20p: Path) -> List[str]:
        return [
            python_exe,
            str(script),
            "--mode",
            mode,
            "--input",
            str(in_path),
            "--output",
            str(outp),
            "--top20",
            str(top20p),
        ]

    # PRIMARY
    rc1 = _run_subprocess(build_cmd(primary_script, out_path, top20_path), log_file, env)
    ok1, miss1 = _assert_output_schema(out_path, required_cols)
    if rc1 == 0 and ok1:
        return (
            RunResult(ok=True, rc=0, script=str(primary_script)),
            None,
            out_path,
            top20_path,
        )

    # FALLBACK (write to __FALLBACK to avoid overwriting primary outputs)
    out_fb = out_path.with_name(out_path.stem + "__FALLBACK" + out_path.suffix)
    top_fb = top20_path.with_name(top20_path.stem + "__FALLBACK" + top20_path.suffix)

    rc2 = _run_subprocess(build_cmd(fallback_script, out_fb, top_fb), log_file, env)
    ok2, miss2 = _assert_output_schema(out_fb, required_cols)
    if rc2 == 0 and ok2:
        return (
            RunResult(ok=False, rc=rc1, script=str(primary_script), error=_fmt_err(rc1, ok1, miss1)),
            RunResult(ok=True, rc=0, script=str(fallback_script)),
            out_fb,
            top_fb,
        )

    # both failed
    return (
        RunResult(ok=False, rc=rc1, script=str(primary_script), error=_fmt_err(rc1, ok1, miss1)),
        RunResult(ok=False, rc=rc2, script=str(fallback_script), error=_fmt_err(rc2, ok2, miss2)),
        out_fb,
        top_fb,
    )


def _fmt_err(rc: int, ok_schema: bool, missing: List[str]) -> str:
    if rc != 0 and ok_schema:
        return f"script_failed_rc={rc}"
    if rc == 0 and not ok_schema:
        return f"output_schema_failed_missing={missing}"
    if rc != 0 and not ok_schema:
        return f"rc={rc}; output_schema_failed_missing={missing}"
    return ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", required=True, choices=["pre", "post", "none"])
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--top20", required=True)

    # NEW: status dir (where RUN_STATUS.* goes)
    ap.add_argument(
        "--status_dir",
        default="",
        help="Directory to write RUN_STATUS.json/txt. Default: output file directory",
    )

    ap.add_argument("--python", default=sys.executable, help="Python executable to run scripts")
    ap.add_argument("--log_dir", default=str(LOG_DIR_DEFAULT), help="Log directory")
    ap.add_argument("--run_id", default="", help="Optional run id. Default: now()")

    # scripts
    ap.add_argument("--primary_script", default=str(BASE_DIR / "daily_auto_run_final.py"))
    ap.add_argument("--fallback_script", default=str(BASE_DIR / "fallback_core" / "daily_auto_run_final.py"))

    args = ap.parse_args()

    mode = args.mode
    in_path = Path(args.input).resolve()
    out_path = Path(args.output).resolve()
    top20_path = Path(args.top20).resolve()

    status_dir = Path(args.status_dir).resolve() if args.status_dir else out_path.parent.resolve()
    python_exe = args.python
    log_dir = Path(args.log_dir).resolve()
    run_id = args.run_id.strip() or mk_run_id()

    primary_script = Path(args.primary_script).resolve()
    fallback_script = Path(args.fallback_script).resolve()

    # prechecks
    if not in_path.exists():
        print(f"Missing input: {in_path}")
        payload = {
            "updated_at": now_ts(),
            "run_id": run_id,
            "overall_status": "FAILED",
            "mode": mode,
            "input": str(in_path),
            "output": str(out_path),
            "top20": str(top20_path),
            "log_file": "",
            "primary": {"script": str(primary_script), "rc": 2, "error": "input_missing"},
        }
        _write_status_files(status_dir, payload)
        return 2

    if not primary_script.exists():
        print(f"Primary script not found: {primary_script}")
        payload = {
            "updated_at": now_ts(),
            "run_id": run_id,
            "overall_status": "FAILED",
            "mode": mode,
            "input": str(in_path),
            "output": str(out_path),
            "top20": str(top20_path),
            "log_file": "",
            "primary": {"script": str(primary_script), "rc": 2, "error": "primary_missing"},
        }
        _write_status_files(status_dir, payload)
        return 2

    if not fallback_script.exists():
        # fallback missing is not fatal if primary succeeds; we'll record it if needed
        pass

    primary_res, fallback_res, final_out, final_top20 = run_oneclick(
        mode=mode,
        in_path=in_path,
        out_path=out_path,
        top20_path=top20_path,
        python_exe=python_exe,
        primary_script=primary_script,
        fallback_script=fallback_script,
        log_dir=log_dir,
        run_id=run_id,
    )

    log_file = (log_dir / f"oneclick_{mode}_{in_path.stem}_{run_id}.log").resolve()

    overall = "SUCCESS" if primary_res.ok or (fallback_res and fallback_res.ok) else "FAILED"
    payload: Dict[str, Any] = {
        "updated_at": now_ts(),
        "run_id": run_id,
        "overall_status": overall,
        "mode": mode,
        "input": str(in_path),
        "output": str(final_out),
        "top20": str(final_top20),
        "log_file": str(log_file),
        "primary": {"script": primary_res.script, "rc": primary_res.rc, "error": primary_res.error},
    }
    if fallback_res is not None:
        payload["fallback"] = {"script": fallback_res.script, "rc": fallback_res.rc, "error": fallback_res.error}

    _write_status_files(status_dir, payload)

    # console summary (keeps your current style)
    if overall == "SUCCESS":
        if primary_res.ok:
            print(f"\n[SUCCESS] Primary run completed. Log: {log_file}")
        else:
            print(f"\n[SUCCESS] Fallback run completed. Log: {log_file}")
        print(f"[STATUS] {status_dir / 'RUN_STATUS.txt'}")
        return 0

    print("\n[FAILED] Primary core failed, and fallback_core also failed.")
    print(f"[PRIMARY ERROR] {primary_res.error}")
    if fallback_res:
        print(f"[FALLBACK ERROR] {fallback_res.error}")
    print(f"[STATUS] {status_dir / 'RUN_STATUS.txt'}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
