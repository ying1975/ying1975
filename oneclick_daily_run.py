# oneclick_daily_run.py
# TradingSystem - Oneclick runner with 2-level protection + hard output validation
#
# Features:
# - PRIMARY run via daily_auto_run_final.py (in base_dir)
# - If PRIMARY fails OR outputs are missing/empty -> FALLBACK run (fallback_core/daily_auto_run_final.py)
# - Log filename includes mode + input stem
# - Writes RUN_STATUS.txt next to output:
#     - result / used_core / returncodes
#     - output/top20 exists + bytes
#     - output/top20 sha256
# - --dry_run: validate only, no execution
# - --self_check: if input missing, auto-create a minimal test CSV at --input path
#
# Usage:
#   python oneclick_daily_run.py --mode pre --input test.csv --output out.csv --top20 top20.csv
#   python oneclick_daily_run.py --dry_run --mode pre --input test.csv --output out.csv
#   python oneclick_daily_run.py --self_check --mode pre --input self_test.csv --output out.csv --top20 top20.csv

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
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
    """
    Hard guard:
    - output must exist and size > 0
    - if top20_path is provided, it must exist and size > 0
    Otherwise raise RuntimeError to trigger fallback or final failure.
    """
    if not output_path.exists():
        raise RuntimeError(f"Output file not created: {output_path}")
    if output_path.stat().st_size == 0:
        raise RuntimeError(f"Output file is empty: {output_path}")

    if top20_path:
        if not top20_path.exists():
            raise RuntimeError(f"Top20 file not created: {top20_path}")
        if top20_path.stat().st_size == 0:
            raise RuntimeError(f"Top20 file is empty: {top20_path}")


def _write_run_status(
    status_path: Path,
    mode: str,
    input_path: Path,
    output_path: Path,
    top20_path: Optional[Path],
    result: str,          # SUCCESS / FAILED
    used_core: str,       # PRIMARY / FALLBACK / NONE
    log_path: Path,
    primary_rc: Optional[int] = None,
    fallback_rc: Optional[int] = None,
    primary_error: Optional[str] = None,
    fallback_error: Optional[str] = None,
) -> None:
    """
    Write a single status file next to output, always overwritten each run.
    Includes existence/size and sha256 (when files exist and are non-empty).
    """
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

    lines = []
    lines.append(f"timestamp: {dt.datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"mode: {mode}")
    lines.append(f"input: {input_path}")
    lines.append(f"output: {output_path}")
    lines.append(f"top20: {top20_path if top20_path else ''}")
    lines.append(f"result: {result}")
    lines.append(f"used_core: {used_core}")
    lines.append(f"log: {log_path}")

    if primary_rc is not None:
        lines.append(f"primary_returncode: {primary_rc}")
    if fallback_rc is not None:
        lines.append(f"fallback_returncode: {fallback_rc}")

    lines.append(f"output_exists: {out_exists}")
    lines.append(f"output_bytes: {out_size}")
    lines.append(f"top20_exists: {top_exists}")
    lines.append(f"top20_bytes: {top_size}")
    lines.append(f"output_sha256: {output_sha256}")
    lines.append(f"top20_sha256: {top20_sha256}")

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
) -> int:
    """
    Returns returncode. Streams stdout/stderr to both console and log file.
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
            "  python oneclick_daily_run.py --mode pre --input test.csv --output out.csv --top20 top20.csv"
        )


def main() -> None:
    p = argparse.ArgumentParser(description="Oneclick daily run with fallback + status + hashing")
    p.add_argument("--mode", default="pre", choices=["pre", "post"], help="Run mode (compat)")
    p.add_argument("--input", default="", help="Input dataset path (.csv/.parquet/.feather)")
    p.add_argument("--output", default="", help="Output enriched dataset path")
    p.add_argument("--top20", default="", help="Optional Top20 output path (.csv recommended)")
    p.add_argument("--base", default="", help="Base dir (default: folder of this script)")
    p.add_argument("--fallback_dir", default="", help="Fallback core dir (default: <base>/fallback_core)")
    p.add_argument("--python", default=sys.executable, help="Python executable path (default: current)")
    p.add_argument("--dry_run", action="store_true", help="Validate args/env only, do not run pipeline")
    p.add_argument("--self_check", action="store_true", help="If input missing, create minimal test CSV at --input then run")
    args = p.parse_args()

    _validate_args(args.input.strip(), args.output.strip())

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
    _write_header(log_path, f"[FALLBACK_DIR] {fallback_dir}")
    _write_header(log_path, f"[INPUT] {input_path}")
    _write_header(log_path, f"[OUTPUT] {output_path}")
    _write_header(log_path, f"[TOP20] {top20_path if top20_path else ''}")

    # Ensure parent dirs exist for outputs
    _ensure_dir(output_path.parent)
    if top20_path:
        _ensure_dir(top20_path.parent)

    # --- dry-run: validate only ---
    if args.dry_run:
        problems = []
        primary_script = base_dir / "daily_auto_run_final.py"
        if not primary_script.exists():
            problems.append(f"Primary script missing: {primary_script}")
        if not input_path.exists():
            problems.append(f"Input missing: {input_path}")

        if not problems:
            _write_run_status(
                status_path=status_path,
                mode=args.mode,
                input_path=input_path,
                output_path=output_path,
                top20_path=top20_path,
                result="SUCCESS",
                used_core="NONE",
                log_path=log_path,
                primary_error="DRY_RUN: OK (no execution)",
            )
            print("[DRY_RUN] OK. No execution performed.")
            print(f"[STATUS] {status_path}")
            return

        msg = "DRY_RUN failed:\n" + "\n".join(f"- {x}" for x in problems)
        _write_run_status(
            status_path=status_path,
            mode=args.mode,
            input_path=input_path,
            output_path=output_path,
            top20_path=top20_path,
            result="FAILED",
            used_core="NONE",
            log_path=log_path,
            primary_error=msg,
        )
        print(msg)
        print(f"[STATUS] {status_path}")
        raise SystemExit(10)

    # --- self-check: ensure test input exists or create it ---
    if args.self_check:
        if not input_path.exists():
            _ensure_dir(input_path.parent)
            input_path.write_text(
                "code,name,market,close,volume,trade_value,turnover,short_used_ratio,margin_used_ratio\n"
                "2330,TSMC,TWSE,600,1000000,600000000,0.08,0.05,0.20\n"
                "2317,HonHai,TWSE,120,2000000,240000000,0.05,0.02,0.10\n"
                "6488,GlobalWafers,TWO,900,300000,270000000,0.10,0.12,0.45\n",
                encoding="utf-8",
            )
            msg = f"[SELF_CHECK] Created test input: {input_path}"
            print(msg)
            _write_header(log_path, msg)

    daily_args = _build_daily_args(input_path, output_path, top20_path, args.mode)

    # -------- Primary run --------
    primary_script = base_dir / "daily_auto_run_final.py"
    if not primary_script.exists():
        err = f"Primary script not found: {primary_script}"
        _write_run_status(
            status_path=status_path,
            mode=args.mode,
            input_path=input_path,
            output_path=output_path,
            top20_path=top20_path,
            result="FAILED",
            used_core="NONE",
            log_path=log_path,
            primary_error=err,
        )
        raise SystemExit(err)

    print(f"\n[PRIMARY] Running: {primary_script}")
    rc1 = _run_subprocess(args.python, primary_script, daily_args, cwd=base_dir, log_path=log_path)

    if rc1 == 0:
        try:
            _check_outputs_or_raise(output_path, top20_path)
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
        except Exception as e_out:
            warn = _banner(f"FALLBACK TRIGGERED: PRIMARY OUTPUT INVALID: {e_out}")
            print(warn)
            _write_header(log_path, warn)
            # fall through to fallback

    else:
        warn = _banner(f"FALLBACK TRIGGERED: PRIMARY FAILED (returncode={rc1})")
        print(warn)
        _write_header(log_path, warn)

    # -------- Fallback run --------
    fallback_script = fallback_dir / "daily_auto_run_final.py"
    if not fallback_script.exists():
        err = f"Fallback script not found: {fallback_script}"
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
            fallback_error=err,
        )
        print(err)
        print(f"[STATUS] {status_path}")
        raise SystemExit(2)

    print(f"\n[FALLBACK] Running: {fallback_script}")
    rc2 = _run_subprocess(args.python, fallback_script, daily_args, cwd=fallback_dir, log_path=log_path)

    if rc2 == 0:
        try:
            _check_outputs_or_raise(output_path, top20_path)
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
        except Exception as e_out:
            msg = f"Fallback rc=0 but outputs invalid: {e_out}"
            _write_header(log_path, msg)
            print(msg)
            # treat as hard failure

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
        primary_error=f"Primary failed or output invalid (returncode={rc1})",
        fallback_error=f"Fallback failed or output invalid (returncode={rc2})",
    )
    print(f"\n[FAILED] Fallback also failed or outputs invalid. Log: {log_path}")
    print(f"[STATUS] {status_path}")
    raise SystemExit(3)


if __name__ == "__main__":
    main()
