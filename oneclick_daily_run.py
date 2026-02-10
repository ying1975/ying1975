# oneclick_daily_run.py
# TradingSystem - Oneclick runner with fallback (Windows-friendly)
#
# What it does:
# 1) Run primary pipeline via subprocess (so imports are clean and deterministic)
# 2) If primary fails, automatically run fallback pipeline from fallback_core/
# 3) Write logs to logs/oneclick_YYYYMMDD_HHMMSS.log
#
# Assumptions (customizable by args):
# - Primary scripts live in this folder (default: current script folder)
# - Fallback scripts live in: <base_dir>/fallback_core/
# - Both primary and fallback provide: daily_auto_run_final.py, strategy_score.py, export_top20.py
#
# Usage example:
#   python oneclick_daily_run.py --mode pre --input test.csv --output out.csv --top20 top20.csv
#
# If you omit --input/--output, it will not guess; it will stop with a clear message.

from __future__ import annotations

import argparse
import datetime as dt
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional, Tuple


def _now_str() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_header(log_path: Path, text: str) -> None:
    _ensure_dir(log_path.parent)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(text + "\n")


def _run_subprocess(
    python_exe: str,
    script_path: Path,
    args: list[str],
    cwd: Path,
    log_path: Path,
) -> Tuple[int, str]:
    """
    Returns (returncode, phase_tag)
    Streams stdout/stderr to both console and log file.
    """
    cmd = [python_exe, str(script_path)] + args

    _write_header(log_path, "\n" + "=" * 80)
    _write_header(log_path, f"[RUN] cwd={cwd}")
    _write_header(log_path, f"[CMD] {' '.join(cmd)}")

    # Use text mode, merge stderr into stdout
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
            # echo to console
            print(line, end="")
            # write to log
            lf.write(line)

    proc.wait()
    return proc.returncode, "OK" if proc.returncode == 0 else "FAIL"


def _build_daily_args(input_path: str, output_path: str, top20_path: Optional[str], mode: str) -> list[str]:
    a = ["--input", input_path, "--output", output_path, "--mode", mode]
    if top20_path:
        a += ["--top20", top20_path]
    return a


def _validate_paths(input_path: Optional[str], output_path: Optional[str]) -> None:
    if not input_path or not output_path:
        raise SystemExit(
            "Missing required args.\n"
            "You must provide: --input <file> --output <file>\n"
            "Example:\n"
            "  python oneclick_daily_run.py --mode pre --input test.csv --output out.csv --top20 top20.csv"
        )


def main() -> None:
    p = argparse.ArgumentParser(description="Oneclick daily run with fallback")
    p.add_argument("--mode", default="pre", choices=["pre", "post"], help="Run mode (compat)")
    p.add_argument("--input", default="", help="Input dataset path (.csv/.parquet/.feather)")
    p.add_argument("--output", default="", help="Output enriched dataset path")
    p.add_argument("--top20", default="", help="Optional Top20 output path (.csv recommended)")
    p.add_argument("--base", default="", help="Base dir (default: folder of this script)")
    p.add_argument("--fallback_dir", default="", help="Fallback core dir (default: <base>/fallback_core)")
    p.add_argument("--python", default=sys.executable, help="Python executable path (default: current)")
    args = p.parse_args()

    _validate_paths(args.input.strip() or None, args.output.strip() or None)

    base_dir = Path(args.base).resolve() if args.base.strip() else Path(__file__).resolve().parent
    fallback_dir = Path(args.fallback_dir).resolve() if args.fallback_dir.strip() else (base_dir / "fallback_core")

    logs_dir = base_dir / "logs"
    log_path = logs_dir / f"oneclick_{_now_str()}.log"
    _ensure_dir(logs_dir)

    _write_header(log_path, f"[START] {dt.datetime.now().isoformat(timespec='seconds')}")
    _write_header(log_path, f"[BASE] {base_dir}")
    _write_header(log_path, f"[FALLBACK] {fallback_dir}")

    input_path = str(Path(args.input).resolve())
    output_path = str(Path(args.output).resolve())
    top20_path = args.top20.strip() or None
    if top20_path:
        top20_path = str(Path(top20_path).resolve())

    daily_args = _build_daily_args(input_path, output_path, top20_path, args.mode)

    # -------- Primary run (current core) --------
    primary_script = base_dir / "daily_auto_run_final.py"
    if not primary_script.exists():
        raise SystemExit(f"Primary script not found: {primary_script}")

    print(f"\n[PRIMARY] Running: {primary_script}")
    rc1, tag1 = _run_subprocess(args.python, primary_script, daily_args, cwd=base_dir, log_path=log_path)

    if rc1 == 0:
        print(f"\n[SUCCESS] Primary run completed. Log: {log_path}")
        return

    print(f"\n[PRIMARY FAILED] returncode={rc1}. Switching to fallback... (Log: {log_path})")

    # -------- Fallback run (fallback_core/) --------
    fallback_script = fallback_dir / "daily_auto_run_final.py"
    if not fallback_script.exists():
        print("\n[FALLBACK NOT AVAILABLE]")
        print(f"Expected fallback script at: {fallback_script}")
        print("Create fallback core folder and place these files inside:")
        print("  - daily_auto_run_final.py")
        print("  - strategy_score.py")
        print("  - export_top20.py")
        print("\nThen re-run the same command.")
        raise SystemExit(2)

    print(f"\n[FALLBACK] Running: {fallback_script}")
    rc2, tag2 = _run_subprocess(args.python, fallback_script, daily_args, cwd=fallback_dir, log_path=log_path)

    if rc2 == 0:
        print(f"\n[SUCCESS] Fallback run completed. Log: {log_path}")
        return

    print(f"\n[FAILED] Fallback also failed (returncode={rc2}). Log: {log_path}")
    raise SystemExit(3)


if __name__ == "__main__":
    main()
