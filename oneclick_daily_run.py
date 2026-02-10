# oneclick_daily_run.py
# TradingSystem - Oneclick runner with fallback (Windows-friendly)
#
# Enhancements:
# 1) Log filename includes mode + input stem
# 2) Write RUN_STATUS.txt next to output file to indicate PRIMARY/FALLBACK and errors
#
# Usage:
#   python oneclick_daily_run.py --mode pre --input test.csv --output out.csv --top20 top20.csv

from __future__ import annotations

import argparse
import datetime as dt
import subprocess
import sys
from pathlib import Path
from typing import Optional, Tuple


def _now_str() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _sanitize_token(s: str, max_len: int = 60) -> str:
    """
    Make a string safe for filenames:
    keep [A-Za-z0-9._-], convert others to '_', and trim.
    """
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


def _write_line(path: Path, text: str) -> None:
    _ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        f.write(text + "\n")


def _write_header(log_path: Path, text: str) -> None:
    _write_line(log_path, text)


def _write_run_status(
    status_path: Path,
    mode: str,
    input_path: Path,
    output_path: Path,
    top20_path: Optional[Path],
    result: str,
    used_core: str,
    log_path: Path,
    primary_rc: Optional[int] = None,
    fallback_rc: Optional[int] = None,
    primary_error: Optional[str] = None,
    fallback_error: Optional[str] = None,
) -> None:
    """
    Write a single status file next to output, always overwritten each run.
    """
    _ensure_dir(status_path.parent)
    lines = []
    lines.append(f"timestamp: {dt.datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"mode: {mode}")
    lines.append(f"input: {input_path}")
    lines.append(f"output: {output_path}")
    lines.append(f"top20: {top20_path if top20_path else ''}")
    lines.append(f"result: {result}")          # SUCCESS / FAILED
    lines.append(f"used_core: {used_core}")    # PRIMARY / FALLBACK / NONE
    lines.append(f"log: {log_path}")

    # Output existence checks (defensive)
    out_exists = output_path.exists()
    out_size = output_path.stat().st_size if out_exists else 0

    top_exists = False
    top_size = 0
    if top20_path:
        top_exists = top20_path.exists()
        top_size = top20_path.stat().st_size if top_exists else 0

    lines.append(f"output_exists: {out_exists}")
    lines.append(f"output_bytes: {out_size}")
    lines.append(f"top20_exists: {top_exists}")
    lines.append(f"top20_bytes: {top_size}")

    if primary_rc is not None:
        lines.append(f"primary_returncode: {primary_rc}")
    if fallback_rc is not None:
        lines.append(f"fallback_returncode: {fallback_rc}")
    if primary_error:
        lines.append("primary_error:")
        lines.append(primary_error)
    if fallback_error:
        lines.append("fallback_error:")
        lines.append(fallback_error)

    status_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


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
    p = argparse.ArgumentParser(description="Oneclick daily run with fallback + status")
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

    input_path = Path(args.input).resolve()
    output_path = Path(args.output).resolve()
    top20_path = Path(args.top20).resolve() if args.top20.strip() else None

    # Status file lives next to output
    status_path = output_path.parent / "RUN_STATUS.txt"

    # Log filename includes mode + input stem (sanitized)
    logs_dir = base_dir / "logs"
    _ensure_dir(logs_dir)
    token_mode = _sanitize_token(args.mode)
    token_input = _sanitize_token(input_path.stem)
    log_path = logs_dir / f"oneclick_{token_mode}_{token_input}_{_now_str()}.log"

    _write_header(log_path, f"[START] {dt.datetime.now().isoformat(timespec='seconds')}")
    _write_header(log_path, f"[BASE] {base_dir}")
    _write_header(log_path, f"[FALLBACK] {fallback_dir}")
    _write_header(log_path, f"[INPUT] {input_path}")
    _write_header(log_path, f"[OUTPUT] {output_path}")
    _write_header(log_path, f"[TOP20] {top20_path if top20_path else ''}")

    daily_args = _build_daily_args(str(input_path), str(output_path), str(top20_path) if top20_path else None, args.mode)

    # -------- Primary run --------
    primary_script = base_dir / "daily_auto_run_final.py"
    if not primary_script.exists():
        _write_run_status(
            status_path=status_path,
            mode=args.mode,
            input_path=input_path,
            output_path=output_path,
            top20_path=top20_path,
            result="FAILED",
            used_core="NONE",
            log_path=log_path,
            primary_error=f"Primary script not found: {primary_script}",
        )
        raise SystemExit(f"Primary script not found: {primary_script}")

    print(f"\n[PRIMARY] Running: {primary_script}")
    rc1, _ = _run_subprocess(args.python, primary_script, daily_args, cwd=base_dir, log_path=log_path)

    if rc1 == 0:
        _write_run_status(
            status_path=status_path,
            mode=args.mode,
            input_path=input_path,
            output_path=output_path,
            top20_path=top20_path,
            result="SUCCESS",
            used_core="PRIMARY",
            log_path=log_path,
            primary_rc=rc1,
        )
        print(f"\n[SUCCESS] Primary run completed. Log: {log_path}")
        print(f"[STATUS] {status_path}")
        return

    print(f"\n[PRIMARY FAILED] returncode={rc1}. Switching to fallback... (Log: {log_path})")

    # -------- Fallback run --------
    fallback_script = fallback_dir / "daily_auto_run_final.py"
    if not fallback_script.exists():
        _write_run_status(
            status_path=status_path,
            mode=args.mode,
            input_path=input_path,
            output_path=output_path,
            top20_path=top20_path,
            result="FAILED",
            used_core="NONE",
            log_path=log_path,
            primary_rc=rc1,
            fallback_error=f"Fallback script not found: {fallback_script}",
        )
        print("\n[FALLBACK NOT AVAILABLE]")
        print(f"Expected fallback script at: {fallback_script}")
        print("Create fallback core folder and place these files inside:")
        print("  - daily_auto_run_final.py")
        print("  - strategy_score.py")
        print("  - export_top20.py")
        raise SystemExit(2)

    print(f"\n[FALLBACK] Running: {fallback_script}")
    rc2, _ = _run_subprocess(args.python, fallback_script, daily_args, cwd=fallback_dir, log_path=log_path)

    if rc2 == 0:
        _write_run_status(
            status_path=status_path,
            mode=args.mode,
            input_path=input_path,
            output_path=output_path,
            top20_path=top20_path,
            result="SUCCESS",
            used_core="FALLBACK",
            log_path=log_path,
            primary_rc=rc1,
            fallback_rc=rc2,
        )
        print(f"\n[SUCCESS] Fallback run completed. Log: {log_path}")
        print(f"[STATUS] {status_path}")
        return

    _write_run_status(
        status_path=status_path,
        mode=args.mode,
        input_path=input_path,
        output_path=output_path,
        top20_path=top20_path,
        result="FAILED",
        used_core="NONE",
        log_path=log_path,
        primary_rc=rc1,
        fallback_rc=rc2,
        primary_error=f"Primary failed with returncode={rc1}",
        fallback_error=f"Fallback failed with returncode={rc2}",
    )
    print(f"\n[FAILED] Fallback also failed (returncode={rc2}). Log: {log_path}")
    print(f"[STATUS] {status_path}")
    raise SystemExit(3)


if __name__ == "__main__":
    main()
