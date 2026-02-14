from __future__ import annotations

import argparse
import csv
import datetime as dt
import sys
from pathlib import Path
from typing import List, Dict, Optional


BASE = Path(__file__).resolve().parent
SUMMARY = BASE / "run_summary.csv"
ALERT = BASE / "HEALTH_ALERT.txt"


def _parse_ts(s: str) -> Optional[dt.datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


def _read_last_rows(max_rows: int) -> List[Dict[str, str]]:
    if not SUMMARY.exists():
        return []
    with SUMMARY.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if max_rows > 0:
        return rows[-max_rows:]
    return rows


def _write_alert(msg: str) -> None:
    ts = dt.datetime.now().isoformat(timespec="seconds")
    ALERT.write_text(f"[{ts}] {msg}\n", encoding="utf-8")


def _clear_alert() -> None:
    if ALERT.exists():
        try:
            ALERT.unlink()
        except Exception:
            pass


def _is_success(r: Dict[str, str]) -> bool:
    return (r.get("result", "") or "").strip().upper() == "SUCCESS"


def _is_failed(r: Dict[str, str]) -> bool:
    return (r.get("result", "") or "").strip().upper() == "FAILED"


def _used_core(r: Dict[str, str]) -> str:
    return (r.get("used_core", "") or "").strip().upper()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="TradingSystem health gate based on run_summary.csv")
    p.add_argument("--window_days", type=int, default=7, help="Window (days) for fallback ratio calculation")
    p.add_argument("--fb_ratio", type=float, default=0.40, help="Alert if fallback ratio > this threshold (0~1)")
    p.add_argument("--min_samples", type=int, default=5, help="Minimum SUCCESS samples required to evaluate ratio")
    p.add_argument("--max_rows", type=int, default=200, help="Read last N rows from run_summary.csv (0 = all)")
    p.add_argument("--recent_hours", type=int, default=48, help="Time window (hours) to check FAILED runs")
    return p


def main() -> int:
    args = build_parser().parse_args()

    rows = _read_last_rows(args.max_rows)
    if not rows:
        _write_alert("No run_summary.csv or empty summary; cannot assess health.")
        return 30

    now = dt.datetime.now()

    # 1) Any FAILED in last recent_hours => alert
    cutoff_failed = now - dt.timedelta(hours=max(args.recent_hours, 1))
    for r in rows:
        t = _parse_ts(r.get("timestamp", ""))
        if t is None:
            continue
        if t >= cutoff_failed and _is_failed(r):
            _write_alert(
                "FAILED detected in recent window.\n"
                f"timestamp={r.get('timestamp','')}\n"
                f"mode={r.get('mode','')}\n"
                f"output={r.get('output','')}\n"
                f"log={r.get('log','')}"
            )
            return 31

    # 2) Fallback ratio over last window_days (SUCCESS only)
    window_days = max(args.window_days, 1)
    cutoff_ratio = now - dt.timedelta(days=window_days)

    succ_in_window: List[Dict[str, str]] = []
    for r in rows:
        t = _parse_ts(r.get("timestamp", ""))
        if t is None:
            continue
        if t < cutoff_ratio:
            continue
        if _is_success(r):
            succ_in_window.append(r)

    # If not enough samples in window, fall back to the most recent SUCCESS rows
    if len(succ_in_window) < args.min_samples:
        succ_in_window = [r for r in rows if _is_success(r)]
        succ_in_window = succ_in_window[-max(args.min_samples, 1):] if succ_in_window else []

    if len(succ_in_window) >= args.min_samples:
        fb_cnt = sum(1 for r in succ_in_window if _used_core(r) == "FALLBACK")
        ratio = fb_cnt / len(succ_in_window)

        if ratio > args.fb_ratio:
            _write_alert(
                "Risk: FALLBACK ratio exceeded threshold (SUCCESS runs only)\n"
                f"fallback_count={fb_cnt}\n"
                f"success_count={len(succ_in_window)}\n"
                f"ratio={ratio:.2%}\n"
                f"threshold={args.fb_ratio:.2%}\n"
                f"window_days={window_days}\n"
                f"window_start={cutoff_ratio.isoformat(timespec='seconds')}"
            )
            return 32

    # Healthy
    _clear_alert()
    return 0


if __name__ == "__main__":
    sys.exit(main())
