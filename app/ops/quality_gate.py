from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


BASE = Path(r"C:\TradingSystem")
IN_CSV = BASE / "data" / "daily_input.csv"
OUT_DIR = BASE / "data" / "out"
REPORT_JSON = OUT_DIR / "quality_report.json"
DEGRADED_FLAG = OUT_DIR / "QUALITY_DEGRADED.txt"


def _atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding=encoding)
    tmp.replace(path)


def _atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False, encoding="utf-8-sig")
    tmp.replace(path)


def main() -> int:
    ap = argparse.ArgumentParser(description="Quality gate for daily_input.csv")
    ap.add_argument(
        "--mode",
        choices=["fail", "degrade"],
        default="fail",
        help="fail: exit nonzero if threshold exceeded; degrade: drop bad rows then continue",
    )
    ap.add_argument("--max_bad_close_pct", type=float, default=0.01, help="e.g. 0.01=1%")
    ap.add_argument("--max_bad_trade_value_pct", type=float, default=0.005, help="e.g. 0.005=0.5%")
    ap.add_argument("--min_rows", type=int, default=800, help="minimum rows after degrade")
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not IN_CSV.exists():
        payload = {"result": "FAIL", "reason": f"missing input: {str(IN_CSV)}"}
        _atomic_write_text(REPORT_JSON, json.dumps(payload, ensure_ascii=False, indent=2) + "\n", "utf-8")
        print(f"[QUALITY_GATE] FAIL: {payload['reason']}")
        return 3

    df = pd.read_csv(IN_CSV, encoding="utf-8-sig")
    n = len(df)

    required = ["close", "trade_value"]
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        payload = {"result": "FAIL", "reason": f"missing columns: {missing_cols}", "has": list(df.columns)}
        _atomic_write_text(REPORT_JSON, json.dumps(payload, ensure_ascii=False, indent=2) + "\n", "utf-8")
        print(f"[QUALITY_GATE] FAIL: {payload['reason']}")
        return 3

    close = pd.to_numeric(df["close"], errors="coerce")
    tv = pd.to_numeric(df["trade_value"], errors="coerce")

    bad_close_mask = close.isna() | (close <= 0)
    bad_tv_mask = tv.isna() | (tv <= 0)

    bad_close = int(bad_close_mask.sum())
    bad_tv = int(bad_tv_mask.sum())

    bad_close_pct = (bad_close / n) if n else 1.0
    bad_tv_pct = (bad_tv / n) if n else 1.0

    payload = {
        "input": str(IN_CSV),
        "rows": n,
        "bad_close": bad_close,
        "bad_close_pct": round(bad_close_pct, 6),
        "bad_trade_value": bad_tv,
        "bad_trade_value_pct": round(bad_tv_pct, 6),
        "thresholds": {
            "max_bad_close_pct": args.max_bad_close_pct,
            "max_bad_trade_value_pct": args.max_bad_trade_value_pct,
            "min_rows": args.min_rows,
        },
        "mode": args.mode,
        "result": "PASS",
    }

    over = (bad_close_pct > args.max_bad_close_pct) or (bad_tv_pct > args.max_bad_trade_value_pct)

    if over:
        if args.mode == "fail":
            payload["result"] = "FAIL"
            _atomic_write_text(REPORT_JSON, json.dumps(payload, ensure_ascii=False, indent=2) + "\n", "utf-8")
            print(
                f"[QUALITY_GATE] FAIL: bad_close={bad_close} ({bad_close_pct:.2%}) "
                f"bad_trade_value={bad_tv} ({bad_tv_pct:.2%}) "
                f"thr_close={args.max_bad_close_pct:.2%} thr_tv={args.max_bad_trade_value_pct:.2%}"
            )
            return 3

        keep = ~(bad_close_mask | bad_tv_mask)
        df2 = df.loc[keep].copy()
        n2 = len(df2)

        payload["result"] = "DEGRADED"
        payload["rows_after"] = n2
        payload["dropped"] = int(n - n2)

        if n2 < args.min_rows:
            payload["result"] = "FAIL"
            payload["reason"] = f"rows_after too few: {n2} < {args.min_rows}"
            _atomic_write_text(REPORT_JSON, json.dumps(payload, ensure_ascii=False, indent=2) + "\n", "utf-8")
            print(f"[QUALITY_GATE] FAIL: {payload['reason']}")
            return 3

        _atomic_write_csv(df2, IN_CSV)
        _atomic_write_text(
            DEGRADED_FLAG,
            f"DEGRADED\nrows_before={n}\nrows_after={n2}\n"
            f"bad_close_pct={bad_close_pct:.6f}\nbad_trade_value_pct={bad_tv_pct:.6f}\n",
            "utf-8",
        )
        _atomic_write_text(REPORT_JSON, json.dumps(payload, ensure_ascii=False, indent=2) + "\n", "utf-8")
        print(f"[QUALITY_GATE] DEGRADED: wrote filtered daily_input.csv rows={n2}")
        return 0

    _atomic_write_text(REPORT_JSON, json.dumps(payload, ensure_ascii=False, indent=2) + "\n", "utf-8")
    print(
        f"[QUALITY_GATE] PASS: bad_close={bad_close} ({bad_close_pct:.2%}) "
        f"bad_trade_value={bad_tv} ({bad_tv_pct:.2%})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
