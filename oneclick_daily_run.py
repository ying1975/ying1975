# oneclick_daily_run.py
# TradingSystem - Oneclick runner with:
# - PRIMARY -> FALLBACK (2-level protection)
# - Hard output validation (exist + bytes > 0)
# - Output schema assertions (required columns)
# - RUN_STATUS.txt + RUN_STATUS.json
# - Log filename includes mode + input stem
# - precheck / dry_run / self_check
# - A) Stability check: compare SHA256 vs previous SUCCESS for same key
# - B) Fallback output tagging: __FALLBACK (does not overwrite primary outputs)
# - run_summary.csv rolling log (keeps last 30 days)
#
# Key upgrades:
# - run_key now includes input_sha256 to avoid false stability alarms when daily_input.csv changes.

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Optional, List, Dict


# -----------------------------
# Config: Output schema contract
# -----------------------------
REQUIRED_OUTPUT_COLS = [
    "code",
    "market",
    "close",
    # at least one of these should exist:
    # "trade_value" OR ("volume" + "close") depending on pipeline; we enforce trade_value by default
    "trade_value",
    # lights (your repo guarantees consistency)
    "light_full",
    "light_decision",
    "light_top20",
]


# -----------------------------
# Utilities
# -----------------------------
def _now_str() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _sanitize_token(s: str, max_len: int = 60) -> str:
    out = []
    for ch in s:
        if ch.isalnum() or ch in "._-":
            out.append(ch)
        else:
            out.append("_")
    t = "".join(out).strip("._-")
    if not t:
        t = "NA"
    return t[:max_len]


def _banner(text: str) -> str:
    bar = "!" * 80
    return f"\n{bar}\n{text}\n{bar}\n"


def _write_line(path: Path, text: str) -> None:
    _ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        f.write(text + "\n")


def _write_header(log_path: Path, text: str) -> None:
    _write_line(log_path, text)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _check_outputs_or_raise(output_path: Path, top20_path: Optional[Path]) -> None:
    if not output_path.exists():
        raise RuntimeError(f"Output file not created: {output_path}")
    if output_path.stat().st_size == 0:
        raise RuntimeError(f"Output file is empty: {output_path}")

    if top20_path:
        if not top20_path.exists():
            raise RuntimeError(f"Top20 file not created: {top20_path}")
        if top20_path.stat().st_size == 0:
            raise RuntimeError(f"Top20 file is empty: {top20_path}")


def _read_csv_header_cols(path: Path) -> List[str]:
    # robust header read for CSV, and strip UTF-8 BOM on first column
    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, [])

    cols = []
    for i, c in enumerate(header):
        if c is None:
            continue
        s = str(c).strip()
        # Strip BOM if present (common when saved with utf-8-sig)
        if i == 0:
            s = s.lstrip("\ufeff")
        cols.append(s)
    return cols


def _assert_output_schema(output_path: Path, required_cols: List[str]) -> None:
    """
    Enforce output column contract to prevent silent failure.
    Currently supports CSV outputs (your current setup).
    """
    if output_path.suffix.lower() != ".csv":
        # If you later use parquet/feather, add implementation here
        return

    cols = _read_csv_header_cols(output_path)
    colset = set(cols)

    missing = [c for c in required_cols if c not in colset]
    if missing:
        raise RuntimeError(
            "Output schema assertion failed: missing required columns.\n"
            f"missing={missing}\n"
            f"output={output_path}\n"
            f"has={cols[:50]}{' ...' if len(cols) > 50 else ''}"
        )


def _tagged_path(path: Path, tag: str) -> Path:
    # out.csv -> out__FALLBACK.csv; out -> out__FALLBACK
    if path.suffix:
        return path.with_name(f"{path.stem}{tag}{path.suffix}")
    return path.with_name(f"{path.name}{tag}")


def _stable_key(mode: str, input_path: Path, input_sha256: str, top20_path: Optional[Path]) -> str:
    # include input_sha256 to avoid false stability alarms when daily_input.csv changes day-to-day
    t = str(top20_path) if top20_path else ""
    return f"{mode}||{str(input_path)}||{input_sha256}||{t}"


def _read_prev_status_json(status_json_path: Path) -> dict:
    if not status_json_path.exists():
        return {}
    try:
        return json.loads(status_json_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _create_min_test_csv(path: Path) -> None:
    _ensure_dir(path.parent)
    path.write_text(
        "code,name,market,close,volume,trade_value,turnover,short_used_ratio,margin_used_ratio\n"
        "2330,TSMC,TWSE,600,1000000,600000000,0.08,0.05,0.20\n"
        "2317,HonHai,TWSE,120,2000000,240000000,0.05,0.02,0.10\n"
        "6488,GlobalWafers,TWO,900,300000,270000000,0.10,0.12,0.45\n",
        encoding="utf-8",
    )


# -----------------------------
# Rolling summary (keep 30 days)
# -----------------------------
SUMMARY_COLS = [
    "timestamp",
    "mode",
    "run_key",
    "input",
    "input_sha256",
    "output",
    "top20",
    "result",
    "used_core",
    "primary_returncode",
    "fallback_returncode",
    "output_exists",
    "output_bytes",
    "top20_exists",
    "top20_bytes",
    "output_sha256",
    "top20_sha256",
    "log",    
    "build_version",
    "build_notes",

]


def _append_run_summary(summary_csv_path: Path, payload: dict, keep_days: int = 30) -> None:
    _ensure_dir(summary_csv_path.parent)

    rows: List[Dict[str, str]] = []
    if summary_csv_path.exists():
        try:
            with summary_csv_path.open("r", newline="", encoding="utf-8") as f:
                r = csv.DictReader(f)
                for row in r:
                    rows.append(row)
        except Exception:
            rows = []

    new_row = {k: str(payload.get(k, "")) for k in SUMMARY_COLS}
    rows.append(new_row)

    cutoff = dt.datetime.now() - dt.timedelta(days=keep_days)

    def _parse_ts(s: str) -> Optional[dt.datetime]:
        if not s:
            return None
        try:
            return dt.datetime.fromisoformat(s.strip())
        except Exception:
            return None

    kept: List[Dict[str, str]] = []
    for row in rows:
        ts = _parse_ts(row.get("timestamp", ""))
        if ts is None:
            kept.append(row)
        else:
            if ts >= cutoff:
                kept.append(row)

    with summary_csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_COLS)
        w.writeheader()
        for row in kept:
            w.writerow({k: row.get(k, "") for k in SUMMARY_COLS})


# -----------------------------
# Status writer (txt + json + summary)
# -----------------------------
def _write_run_status(
    *,
    status_path: Path,
    summary_csv_path: Path,
    mode: str,
    run_key: str,
    input_path: Path,
    input_sha256: str,
    output_path: Path,
    top20_path: Optional[Path],
    result: str,          # SUCCESS / FAILED
    used_core: str,       # PRIMARY / FALLBACK / NONE
    log_path: Path,
    primary_rc: Optional[int] = None,
    fallback_rc: Optional[int] = None,
    primary_error: str = "",
    fallback_error: str = "",
) -> None:
    _ensure_dir(status_path.parent)

    out_exists = output_path.exists()
    out_size = output_path.stat().st_size if out_exists else 0

    top_exists = False
    top_size = 0
    if top20_path:
        top_exists = top20_path.exists()
        top_size = top20_path.stat().st_size if top_exists else 0

    output_sha256 = _sha256_file(output_path) if out_exists and out_size > 0 else ""
    top20_sha256 = _sha256_file(top20_path) if top20_path and top_exists and top_size > 0 else ""

    timestamp = dt.datetime.now().isoformat(timespec="seconds")

    # TXT
    lines = [
        f"timestamp: {timestamp}",
        f"mode: {mode}",
        f"run_key: {run_key}",
        f"input: {input_path}",
        f"input_sha256: {input_sha256}",
        f"output: {output_path}",
        f"top20: {top20_path if top20_path else ''}",
        f"result: {result}",
        f"used_core: {used_core}",
        f"log: {log_path}",
        f"primary_returncode: {primary_rc if primary_rc is not None else ''}",
        f"fallback_returncode: {fallback_rc if fallback_rc is not None else ''}",
        f"output_exists: {out_exists}",
        f"output_bytes: {out_size}",
        f"top20_exists: {top_exists}",
        f"top20_bytes: {top_size}",
        f"output_sha256: {output_sha256}",
        f"top20_sha256: {top20_sha256}",
    ]
    if primary_error:
        lines += ["primary_error:", primary_error]
    if fallback_error:
        lines += ["fallback_error:", fallback_error]

    status_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    # JSON
    status_json_path = status_path.with_suffix(".json")
  
  # build info
    build_info_path = Path(__file__).resolve().parent / "BUILD_INFO.json"
    build_version = ""
    build_notes = ""
    try:
        if build_info_path.exists():
            bi = json.loads(build_info_path.read_text(encoding="utf-8"))
            build_version = str(bi.get("version", ""))
            build_notes = str(bi.get("notes", ""))
    except Exception:
        pass

    payload = {
        "timestamp": timestamp,
        "mode": mode,
        "run_key": run_key,
        "input": str(input_path),
        "input_sha256": input_sha256,
        "output": str(output_path),
        "top20": str(top20_path) if top20_path else "",
        "result": result,
        "used_core": used_core,
        "log": str(log_path),
        "primary_returncode": primary_rc,
        "fallback_returncode": fallback_rc,
        "output_exists": out_exists,
        "output_bytes": out_size,
        "top20_exists": top_exists,
        "top20_bytes": top_size,
        "output_sha256": output_sha256,
        "top20_sha256": top20_sha256,
        "primary_error": primary_error,
        "fallback_error": fallback_error,
        "build_version": build_version,
        "build_notes": build_notes,
    }
    status_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    # Summary (rolling 30 days)
    _append_run_summary(summary_csv_path, payload, keep_days=30)


# -----------------------------
# Stability check (A)
# -----------------------------
def _stability_fail(
    *,
    status_path: Path,
    summary_csv_path: Path,
    log_path: Path,
    mode: str,
    run_key: str,
    input_path: Path,
    input_sha256: str,
    output_path: Path,
    top20_path: Optional[Path],
    used_core: str,
    primary_rc: Optional[int],
    fallback_rc: Optional[int],
    msg: str,
    exit_code: int,
) -> None:
    banner = _banner(msg)
    print(banner)
    _write_header(log_path, banner)
    _write_run_status(
        status_path=status_path,
        summary_csv_path=summary_csv_path,
        mode=mode,
        run_key=run_key,
        input_path=input_path,
        input_sha256=input_sha256,
        output_path=output_path,
        top20_path=top20_path,
        result="FAILED",
        used_core=used_core,
        log_path=log_path,
        primary_rc=primary_rc,
        fallback_rc=fallback_rc,
        primary_error=msg if used_core == "PRIMARY" else "",
        fallback_error=msg if used_core == "FALLBACK" else "",
    )
    raise SystemExit(exit_code)


def _stability_compare_or_fail(
    *,
    prev: dict,
    status_path: Path,
    summary_csv_path: Path,
    log_path: Path,
    mode: str,
    run_key: str,
    input_path: Path,
    input_sha256: str,
    output_path: Path,
    top20_path: Optional[Path],
    used_core: str,
    primary_rc: Optional[int],
    fallback_rc: Optional[int],
) -> None:
    if prev.get("run_key", "") != run_key:
        return
    if prev.get("result", "") != "SUCCESS":
        return

    prev_out_sha = (prev.get("output_sha256", "") or "").strip()
    if prev_out_sha:
        now_out_sha = _sha256_file(output_path)
        if now_out_sha != prev_out_sha:
            msg = (
                "STABILITY CHECK FAILED: output sha256 changed\n"
                f"prev={prev_out_sha}\n"
                f"now ={now_out_sha}\n"
                f"key ={run_key}"
            )
            _stability_fail(
                status_path=status_path,
                summary_csv_path=summary_csv_path,
                log_path=log_path,
                mode=mode,
                run_key=run_key,
                input_path=input_path,
                input_sha256=input_sha256,
                output_path=output_path,
                top20_path=top20_path,
                used_core=used_core,
                primary_rc=primary_rc,
                fallback_rc=fallback_rc,
                msg=msg,
                exit_code=20,
            )

    prev_top_sha = (prev.get("top20_sha256", "") or "").strip()
    if top20_path and prev_top_sha:
        now_top_sha = _sha256_file(top20_path)
        if now_top_sha != prev_top_sha:
            msg = (
                "STABILITY CHECK FAILED: top20 sha256 changed\n"
                f"prev={prev_top_sha}\n"
                f"now ={now_top_sha}\n"
                f"key ={run_key}"
            )
            _stability_fail(
                status_path=status_path,
                summary_csv_path=summary_csv_path,
                log_path=log_path,
                mode=mode,
                run_key=run_key,
                input_path=input_path,
                input_sha256=input_sha256,
                output_path=output_path,
                top20_path=top20_path,
                used_core=used_core,
                primary_rc=primary_rc,
                fallback_rc=fallback_rc,
                msg=msg,
                exit_code=21,
            )


# -----------------------------
# Runner
# -----------------------------
def _run_subprocess(python_exe: str, script_path: Path, args: list[str], cwd: Path, log_path: Path) -> int:
    cmd = [python_exe, str(script_path)] + args
    _write_header(log_path, "\n" + "=" * 80)
    _write_header(log_path, f"[RUN] cwd={cwd}")
    _write_header(log_path, f"[CMD] {' '.join(cmd)}")

    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    assert proc.stdout is not None
    with log_path.open("a", encoding="utf-8") as lf:
        for line in proc.stdout:
            print(line, end="")
            lf.write(line)

    proc.wait()
    return proc.returncode


def _build_daily_args(input_path: Path, output_path: Path, top20_path: Optional[Path], mode: str) -> list[str]:
    a = ["--input", str(input_path), "--output", str(output_path), "--mode", mode]
    if top20_path:
        a += ["--top20", str(top20_path)]
    return a


def _validate_args(input_s: str, output_s: str) -> None:
    if not input_s or not output_s:
        raise SystemExit(
            "Missing required args.\n"
            "You must provide: --input <file> --output <file>\n"
            "Example:\n"
            "  python oneclick_daily_run.py --mode pre --input daily_input.csv --output daily_out.csv --top20 daily_top20.csv"
        )


def main() -> None:
    p = argparse.ArgumentParser(description="Oneclick daily run with fallback + status + stability(A) + fallback tagging(B) + 30d summary + schema assertions")
    p.add_argument("--mode", default="pre", choices=["pre", "post"], help="Run mode (compat)")
    p.add_argument("--input", default="", help="Input dataset path (.csv/.parquet/.feather)")
    p.add_argument("--output", default="", help="Output enriched dataset path")
    p.add_argument("--top20", default="", help="Optional Top20 output path (.csv recommended)")
    p.add_argument("--base", default="", help="Base dir (default: folder of this script)")
    p.add_argument("--fallback_dir", default="", help="Fallback core dir (default: <base>/fallback_core)")
    p.add_argument("--python", default=sys.executable, help="Python executable path (default: current)")
    p.add_argument("--dry_run", action="store_true", help="Validate args/env only, do not run pipeline")
    p.add_argument("--self_check", action="store_true", help="If input missing, create minimal test CSV at --input then run")
    p.add_argument("--precheck", action="store_true", help="Premarket one-command check (create input if missing; validate outputs)")
    args = p.parse_args()

    _validate_args(args.input.strip(), args.output.strip())

    base_dir = Path(args.base).resolve() if args.base.strip() else Path(__file__).resolve().parent
    fallback_dir = Path(args.fallback_dir).resolve() if args.fallback_dir.strip() else (base_dir / "fallback_core")

    input_path = Path(args.input).resolve()
    output_path_primary = Path(args.output).resolve()
    top20_path_primary = Path(args.top20).resolve() if args.top20.strip() else None

    # Central rolling summary (always in base_dir)
    summary_csv_path = base_dir / "run_summary.csv"

    # Logging
    logs_dir = base_dir / "logs"
    _ensure_dir(logs_dir)
    token_mode = _sanitize_token(args.mode)
    token_input = _sanitize_token(input_path.stem)
    log_path = logs_dir / f"oneclick_{token_mode}_{token_input}_{_now_str()}.log"

    _write_header(log_path, f"[START] {dt.datetime.now().isoformat(timespec='seconds')}")
    _write_header(log_path, f"[BASE] {base_dir}")
    _write_header(log_path, f"[FALLBACK_DIR] {fallback_dir}")
    _write_header(log_path, f"[INPUT] {input_path}")
    _write_header(log_path, f"[OUTPUT_PRIMARY] {output_path_primary}")
    _write_header(log_path, f"[TOP20_PRIMARY] {top20_path_primary if top20_path_primary else ''}")

    _ensure_dir(output_path_primary.parent)
    if top20_path_primary:
        _ensure_dir(top20_path_primary.parent)

    # Status paths (primary) – written next to output path
    status_path_primary = output_path_primary.parent / "RUN_STATUS.txt"
    prev_status_json_primary = status_path_primary.with_suffix(".json")

    # precompute input_sha256 if input exists (after precheck/self_check it will exist)
    input_sha256 = ""

    # --- precheck ---
    if args.precheck:
        if not input_path.exists():
            _create_min_test_csv(input_path)
            msg = f"[PRECHECK] Created test input: {input_path}"
            print(msg)
            _write_header(log_path, msg)

        if top20_path_primary is None:
            top20_path_primary = output_path_primary.with_name("top20_precheck.csv")
            _ensure_dir(top20_path_primary.parent)
            msg = f"[PRECHECK] top20 not provided, using: {top20_path_primary}"
            print(msg)
            _write_header(log_path, msg)

    # --- dry-run ---
    if args.dry_run:
        problems = []
        primary_script = base_dir / "daily_auto_run_final.py"
        if not primary_script.exists():
            problems.append(f"Primary script missing: {primary_script}")
        if not input_path.exists():
            problems.append(f"Input missing: {input_path}")

        if not problems:
            input_sha256 = _sha256_file(input_path) if input_path.exists() else ""
            run_key_primary = _stable_key(args.mode, input_path, input_sha256, top20_path_primary)
            _write_run_status(
                status_path=status_path_primary,
                summary_csv_path=summary_csv_path,
                mode=args.mode,
                run_key=run_key_primary,
                input_path=input_path,
                input_sha256=input_sha256,
                output_path=output_path_primary,
                top20_path=top20_path_primary,
                result="SUCCESS",
                used_core="NONE",
                log_path=log_path,
                primary_error="DRY_RUN: OK (no execution)",
            )
            print("[DRY_RUN] OK. No execution performed.")
            print(f"[STATUS] {status_path_primary}")
            return

        msg = "DRY_RUN failed:\n" + "\n".join(f"- {x}" for x in problems)
        input_sha256 = _sha256_file(input_path) if input_path.exists() else ""
        run_key_primary = _stable_key(args.mode, input_path, input_sha256, top20_path_primary)
        _write_run_status(
            status_path=status_path_primary,
            summary_csv_path=summary_csv_path,
            mode=args.mode,
            run_key=run_key_primary,
            input_path=input_path,
            input_sha256=input_sha256,
            output_path=output_path_primary,
            top20_path=top20_path_primary,
            result="FAILED",
            used_core="NONE",
            log_path=log_path,
            primary_error=msg,
        )
        print(msg)
        print(f"[STATUS] {status_path_primary}")
        raise SystemExit(10)

    # --- self-check ---
    if args.self_check and (not input_path.exists()):
        _create_min_test_csv(input_path)
        msg = f"[SELF_CHECK] Created test input: {input_path}"
        print(msg)
        _write_header(log_path, msg)

    # compute input_sha256 now (for run_key + summary)
    input_sha256 = _sha256_file(input_path) if input_path.exists() else ""

    run_key_primary = _stable_key(args.mode, input_path, input_sha256, top20_path_primary)
    prev_primary = _read_prev_status_json(prev_status_json_primary)
    _write_header(log_path, f"[RUN_KEY_PRIMARY] {run_key_primary}")
    _write_header(log_path, f"[INPUT_SHA256] {input_sha256}")

    # ========== PRIMARY RUN ==========
    primary_script = base_dir / "daily_auto_run_final.py"
    if not primary_script.exists():
        err = f"Primary script not found: {primary_script}"
        _write_run_status(
            status_path=status_path_primary,
            summary_csv_path=summary_csv_path,
            mode=args.mode,
            run_key=run_key_primary,
            input_path=input_path,
            input_sha256=input_sha256,
            output_path=output_path_primary,
            top20_path=top20_path_primary,
            result="FAILED",
            used_core="NONE",
            log_path=log_path,
            primary_error=err,
        )
        raise SystemExit(err)

    daily_args_primary = _build_daily_args(input_path, output_path_primary, top20_path_primary, args.mode)

    print(f"\n[PRIMARY] Running: {primary_script}")
    rc1 = _run_subprocess(args.python, primary_script, daily_args_primary, cwd=base_dir, log_path=log_path)

    fallback_reason = ""
    if rc1 == 0:
        try:
            _check_outputs_or_raise(output_path_primary, top20_path_primary)

            # schema assertion (prevents silent schema regressions)
            _assert_output_schema(output_path_primary, REQUIRED_OUTPUT_COLS)

            # Stability (A) – only compares against previous SUCCESS with same run_key
            _stability_compare_or_fail(
                prev=prev_primary,
                status_path=status_path_primary,
                summary_csv_path=summary_csv_path,
                log_path=log_path,
                mode=args.mode,
                run_key=run_key_primary,
                input_path=input_path,
                input_sha256=input_sha256,
                output_path=output_path_primary,
                top20_path=top20_path_primary,
                used_core="PRIMARY",
                primary_rc=rc1,
                fallback_rc=None,
            )

            _write_run_status(
                status_path=status_path_primary,
                summary_csv_path=summary_csv_path,
                mode=args.mode,
                run_key=run_key_primary,
                input_path=input_path,
                input_sha256=input_sha256,
                output_path=output_path_primary,
                top20_path=top20_path_primary,
                result="SUCCESS",
                used_core="PRIMARY",
                log_path=log_path,
                primary_rc=rc1,
            )
            print(f"\n[SUCCESS] Primary run completed. Log: {log_path}")
            print(f"[STATUS] {status_path_primary}")
            return
        except Exception as e_out:
            fallback_reason = f"PRIMARY OUTPUT INVALID: {e_out}"
    else:
        fallback_reason = f"PRIMARY FAILED (returncode={rc1})"

    warn = _banner(f"FALLBACK TRIGGERED: {fallback_reason}\nFallback outputs will be tagged with __FALLBACK")
    print(warn)
    _write_header(log_path, warn)

    # ========== FALLBACK RUN (B: tagged outputs) ==========
    fallback_script = fallback_dir / "daily_auto_run_final.py"
    if not fallback_script.exists():
        err = f"Fallback script not found: {fallback_script}"
        _write_run_status(
            status_path=status_path_primary,
            summary_csv_path=summary_csv_path,
            mode=args.mode,
            run_key=run_key_primary,
            input_path=input_path,
            input_sha256=input_sha256,
            output_path=output_path_primary,
            top20_path=top20_path_primary,
            result="FAILED",
            used_core="NONE",
            log_path=log_path,
            primary_rc=rc1,
            fallback_error=err,
        )
        print(err)
        print(f"[STATUS] {status_path_primary}")
        raise SystemExit(2)

    output_path_fb = _tagged_path(output_path_primary, "__FALLBACK")
    top20_path_fb = _tagged_path(top20_path_primary, "__FALLBACK") if top20_path_primary else None
    _ensure_dir(output_path_fb.parent)
    if top20_path_fb:
        _ensure_dir(top20_path_fb.parent)

    status_path_fb = output_path_fb.parent / "RUN_STATUS.txt"
    prev_status_json_fb = status_path_fb.with_suffix(".json")

    run_key_fb = _stable_key(args.mode, input_path, input_sha256, top20_path_fb)
    prev_fb = _read_prev_status_json(prev_status_json_fb)

    _write_header(log_path, f"[OUTPUT_FALLBACK] {output_path_fb}")
    _write_header(log_path, f"[TOP20_FALLBACK] {top20_path_fb if top20_path_fb else ''}")
    _write_header(log_path, f"[RUN_KEY_FALLBACK] {run_key_fb}")

    daily_args_fb = _build_daily_args(input_path, output_path_fb, top20_path_fb, args.mode)

    print(f"\n[FALLBACK] Running: {fallback_script}")
    rc2 = _run_subprocess(args.python, fallback_script, daily_args_fb, cwd=fallback_dir, log_path=log_path)

    if rc2 == 0:
        try:
            _check_outputs_or_raise(output_path_fb, top20_path_fb)

            # schema assertion
            _assert_output_schema(output_path_fb, REQUIRED_OUTPUT_COLS)

            # stability compare for fallback outputs (separate key)
            _stability_compare_or_fail(
                prev=prev_fb,
                status_path=status_path_fb,
                summary_csv_path=summary_csv_path,
                log_path=log_path,
                mode=args.mode,
                run_key=run_key_fb,
                input_path=input_path,
                input_sha256=input_sha256,
                output_path=output_path_fb,
                top20_path=top20_path_fb,
                used_core="FALLBACK",
                primary_rc=rc1,
                fallback_rc=rc2,
            )

            _write_run_status(
                status_path=status_path_fb,
                summary_csv_path=summary_csv_path,
                mode=args.mode,
                run_key=run_key_fb,
                input_path=input_path,
                input_sha256=input_sha256,
                output_path=output_path_fb,
                top20_path=top20_path_fb,
                result="SUCCESS",
                used_core="FALLBACK",
                log_path=log_path,
                primary_rc=rc1,
                fallback_rc=rc2,
                primary_error=fallback_reason,
            )
            print(f"\n[SUCCESS] Fallback run completed. Log: {log_path}")
            print(f"[OUTPUT] {output_path_fb}")
            if top20_path_fb:
                print(f"[TOP20]  {top20_path_fb}")
            print(f"[STATUS] {status_path_fb}")
            return
        except Exception as e_out:
            msg = f"Fallback rc=0 but outputs invalid: {e_out}"
            _write_header(log_path, msg)
            print(msg)

    _write_run_status(
        status_path=status_path_fb,
        summary_csv_path=summary_csv_path,
        mode=args.mode,
        run_key=run_key_fb,
        input_path=input_path,
        input_sha256=input_sha256,
        output_path=output_path_fb,
        top20_path=top20_path_fb,
        result="FAILED",
        used_core="NONE",
        log_path=log_path,
        primary_rc=rc1,
        fallback_rc=rc2,
        primary_error=fallback_reason,
        fallback_error=f"Fallback failed or output invalid (returncode={rc2})",
    )
    print(f"\n[FAILED] Fallback also failed or outputs invalid. Log: {log_path}")
    print(f"[STATUS] {status_path_fb}")
    raise SystemExit(3)


if __name__ == "__main__":
    main()
