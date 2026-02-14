from __future__ import annotations

import argparse
import csv
import datetime as dt
import sys
from pathlib import Path
from typing import Dict, List, Optional


BASE = Path(r"C:\TradingSystem")
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


def _read_rows(max_rows: int) -> List[Dict[str, str]]:
    if not SUMMARY.exists():
        return []
    with SUMMARY.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return rows[-max_rows:] if max_rows > 0 else rows


def _write_alert(msg: str) -> None:
    ts = dt.datetime.now().isoformat(timespec="seconds")
    ALERT.write_text(f"[{ts}]\n{msg}\n", encoding="utf-8")


def _clear_alert() -> None:
    if ALERT.exists():
        try:
            ALERT.unlink()
        except Exception:
            pass


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--window_days", type=int, default=7)
    ap.add_argument("--fb_ratio", type=float, default=0.40)      # fallback ratio threshold (SUCCESS only)
    ap.add_argument("--min_samples", type=int, default=5)        # minimum SUCCESS samples to evaluate ratio
    ap.add_argument("--recent_hours", type=int, default=48)      # FAILED check window
    ap.add_argument("--max_rows", type=int, default=400)
    args = ap.parse_args()

    rows = _read_rows(args.max_rows)
    if not rows:
        _write_alert("No run_summary.csv; health unknown.")
        return 30

    now = dt.datetime.now()

    # 1) FAILED in recent window => FAIL
    cutoff_failed = now - dt.timedelta(hours=max(args.recent_hours, 1))
    for r in rows:
        t = _parse_ts(r.get("timestamp", ""))
        if t and t >= cutoff_failed:
            if (r.get("result", "") or "").strip().upper() == "FAILED":
                _write_alert(
                    "FAILED detected in recent window\n"
                    f"timestamp={r.get('timestamp','')}\n"
                    f"used_core={r.get('used_core','')}\n"
                    f"output={r.get('output','')}\n"
                    f"log={r.get('log','')}"
                )
                return 31

    # 2) fallback ratio in last window_days (SUCCESS only)
    cutoff = now - dt.timedelta(days=max(args.window_days, 1))
    succ = []
    for r in rows:
        t = _parse_ts(r.get("timestamp", ""))
        if not t or t < cutoff:
            continue
        if (r.get("result", "") or "").strip().upper() == "SUCCESS":
            succ.append(r)

    if len(succ) < args.min_samples:
        # fallback to recent SUCCESS
        succ = [r for r in rows if (r.get("result", "") or "").strip().upper() == "SUCCESS"]
        succ = succ[-max(args.min_samples, 1):] if succ else []

    if len(succ) >= args.min_samples:
        fb_cnt = sum(1 for r in succ if (r.get("used_core", "") or "").strip().upper() == "FALLBACK")
        ratio = fb_cnt / len(succ)
        if ratio > args.fb_ratio:
            _write_alert(
                "FALLBACK ratio exceeded threshold\n"
                f"window_days={args.window_days}\n"
                f"threshold={args.fb_ratio:.2%}\n"
                f"fallback_count={fb_cnt}\n"
                f"success_count={len(succ)}\n"
                f"ratio={ratio:.2%}\n"
            )
            return 32

    _clear_alert()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
