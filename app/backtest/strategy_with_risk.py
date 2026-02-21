from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

from app.ops.path_display import display_path

# Ensure project root is importable (C:\TradingSystem)
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Default backtest snapshot root (can be overridden via --bt_root)
BT_TMP = ROOT / "data" / "out" / "_bt_tmp"


@dataclass
class Costs:
    commission: float = 0.001425
    sell_tax: float = 0.003
    slippage: float = 0.0005


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows: List[Dict[str, str]] = []
        for r in reader:
            rr: Dict[str, str] = {}
            for k, v in r.items():
                if k is None:
                    continue
                rr[k.lstrip("\ufeff")] = v
            rows.append(rr)
        return rows


def _as_float(x: str) -> float:
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return float("nan")


def _as_int(x: str) -> int:
    try:
        return int(float(str(x).strip()))
    except Exception:
        return 0


def _list_days(bt_root: Path) -> List[str]:
    """
    Return sorted YYYYMMDD subdirectories which contain both:
      - daily_out.csv
      - daily_top20.csv
    """
    if not bt_root.exists():
        return []
    days: List[str] = []
    for p in bt_root.iterdir():
        if p.is_dir() and p.name.isdigit() and len(p.name) == 8:
            if (p / "daily_out.csv").exists() and (p / "daily_top20.csv").exists():
                days.append(p.name)
    return sorted(days)


def _breadth_ratio(daily_out_rows: List[Dict[str, str]]) -> float:
    """
    Breadth = fraction of rows where light_decision is truthy/1.
    """
    if not daily_out_rows:
        return 0.0
    pos = 0
    n = 0
    for r in daily_out_rows:
        v = (r.get("light_decision", "") or "").strip()
        if v == "":
            continue
        n += 1
        if v in ("1", "TRUE", "True", "true", "Y", "y"):
            pos += 1
    if n == 0:
        return 0.0
    return pos / n


def _topN_codes(top20_rows: List[Dict[str, str]], topN: int) -> List[str]:
    codes: List[str] = []
    for r in top20_rows:
        c = (r.get("code", "") or "").strip()
        if c:
            codes.append(c)
        if len(codes) >= topN:
            break
    return codes


def _price_map(daily_out_rows: List[Dict[str, str]]) -> Dict[str, float]:
    m: Dict[str, float] = {}
    for r in daily_out_rows:
        c = (r.get("code", "") or "").strip()
        if not c:
            continue
        px = _as_float(r.get("close", "nan"))
        if math.isfinite(px) and px > 0:
            m[c] = px
    return m


def _avg_ret(codes: List[str], px_t: Dict[str, float], px_t1: Dict[str, float]) -> Tuple[float, int, int, int]:
    """
    Returns:
      mean close-to-close return for available codes,
      used, miss_today, miss_next
    """
    rets: List[float] = []
    miss_today = 0
    miss_next = 0
    bad = 0

    for c in codes:
        p0 = px_t.get(c)
        p1 = px_t1.get(c)
        if p0 is None:
            miss_today += 1
            continue
        if p1 is None:
            miss_next += 1
            continue
        if p0 <= 0 or p1 <= 0:
            bad += 1
            continue
        rets.append(p1 / p0 - 1.0)

    if not rets:
        return 0.0, 0, miss_today, miss_next
    return sum(rets) / len(rets), len(rets), miss_today, miss_next


def _apply_costs(gross_ret: float, costs: Costs) -> float:
    """
    Practical approximation:
      entry factor  = (1 - commission - slippage)
      exit factor   = (1 - commission - sell_tax - slippage)
      net_ret = (1+gross_ret)*entry*exit - 1
    """
    entry = 1.0 - (costs.commission + costs.slippage)
    exit_ = 1.0 - (costs.commission + costs.sell_tax + costs.slippage)
    return (1.0 + gross_ret) * entry * exit_ - 1.0


def _stats(equity: List[float]) -> Dict[str, float]:
    if len(equity) < 2:
        return {
            "final_equity": equity[-1] if equity else 0.0,
            "total_return": 0.0,
            "sharpe": 0.0,
            "max_drawdown": 0.0,
        }

    rets: List[float] = []
    for i in range(1, len(equity)):
        if equity[i - 1] <= 0:
            rets.append(0.0)
        else:
            rets.append(equity[i] / equity[i - 1] - 1.0)

    avg = sum(rets) / len(rets)
    var = sum((r - avg) ** 2 for r in rets) / max(1, len(rets) - 1)
    vol = math.sqrt(var)

    ann_ret = avg * 252.0
    ann_vol = vol * math.sqrt(252.0)
    sharpe = (ann_ret / ann_vol) if ann_vol > 0 else 0.0

    peak = equity[0]
    mdd = 0.0
    for x in equity:
        peak = max(peak, x)
        if peak > 0:
            dd = (peak - x) / peak
            mdd = max(mdd, dd)

    return {
        "final_equity": float(equity[-1]),
        "total_return": float(equity[-1] / equity[0] - 1.0),
        "avg_daily_return": float(avg),
        "daily_volatility": float(vol),
        "annual_return_est": float(ann_ret),
        "annual_vol_est": float(ann_vol),
        "sharpe": float(sharpe),
        "max_drawdown": float(mdd),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run_id", required=True)
    ap.add_argument("--out_dir", required=True, help=r"RUN_DIR, e.g. C:\TradingSystem\data\out\runs\20260217_131200")
    ap.add_argument("--strategy", default="trade_value", choices=["trade_value"])
    ap.add_argument("--topN", type=int, default=5)
    ap.add_argument("--risk_on", type=float, default=0.55)
    ap.add_argument("--risk_mid", type=float, default=0.48)  # reserved; kept for compatibility
    ap.add_argument("--initial_equity", type=float, default=1_000_000.0)
    ap.add_argument("--bt_root", default=str(BT_TMP), help="Folder containing YYYYMMDD subdirs with daily_out/top20")
    ap.add_argument("--commission", type=float, default=0.001425)
    ap.add_argument("--sell_tax", type=float, default=0.003)
    ap.add_argument("--slippage", type=float, default=0.0005)
    args = ap.parse_args()

    run_id = args.run_id.strip()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    reports_dir = out_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    bt_root = Path(args.bt_root).resolve()
    days = _list_days(bt_root)
    if len(days) < 2:
        print(f"[BACKTEST] Not enough prepared snapshots (need >= 2 days). bt_root={bt_root}")
        return 1

    costs = Costs(commission=args.commission, sell_tax=args.sell_tax, slippage=args.slippage)

    dates = days[:-1]
    next_dates = days[1:]

    eq_none = [args.initial_equity]
    eq_tier = [args.initial_equity]
    eq_cont = [args.initial_equity]

    rows_out: List[Dict[str, str]] = []

    traded_none = 0
    traded_tier = 0
    traded_cont = 0
    no_trade_tier = 0
    no_trade_cont = 0

    for d0, d1 in zip(dates, next_dates):
        p0 = bt_root / d0
        p1 = bt_root / d1

        out0 = _read_csv_rows(p0 / "daily_out.csv")
        top0 = _read_csv_rows(p0 / "daily_top20.csv")
        out1 = _read_csv_rows(p1 / "daily_out.csv")

        br = _breadth_ratio(out0)

        expo_none = 1.0
        expo_tier = 1.0 if br >= args.risk_on else 0.0
        expo_cont = max(0.0, min(1.0, br))

        px0 = _price_map(out0)
        px1 = _price_map(out1)
        codes = _topN_codes(top0, args.topN)

        gross_ret, used, miss_today, miss_next = _avg_ret(codes, px0, px1)
        net_ret = _apply_costs(gross_ret, costs) if used > 0 else 0.0

        if used > 0:
            traded_none += 1

        if expo_tier > 0 and used > 0:
            traded_tier += 1
        else:
            no_trade_tier += 1

        if expo_cont > 0 and used > 0:
            traded_cont += 1
        else:
            no_trade_cont += 1

        r_none = expo_none * net_ret
        r_tier = expo_tier * net_ret
        r_cont = expo_cont * net_ret

        eq_none.append(eq_none[-1] * (1.0 + r_none))
        eq_tier.append(eq_tier[-1] * (1.0 + r_tier))
        eq_cont.append(eq_cont[-1] * (1.0 + r_cont))

        print(
            f"[RISK_EQ] {d0} br={br:.3f} expo_none={expo_none:.2f} expo_tier={expo_tier:.2f} "
            f"expo_cont={expo_cont:.2f} used={used} ret={net_ret:.5f}"
        )

        rows_out.append(
            {
                "date": d0,
                "next_date": d1,
                "breadth": f"{br:.6f}",
                "used": str(used),
                "miss_today": str(miss_today),
                "miss_next": str(miss_next),
                "net_ret": f"{net_ret:.8f}",
                "expo_none": f"{expo_none:.6f}",
                "expo_tier": f"{expo_tier:.6f}",
                "expo_cont": f"{expo_cont:.6f}",
                "eq_none": f"{eq_none[-2]:.6f}",
                "eq_tier": f"{eq_tier[-2]:.6f}",
                "eq_cont": f"{eq_cont[-2]:.6f}",
                "eq_none_next": f"{eq_none[-1]:.6f}",
                "eq_tier_next": f"{eq_tier[-1]:.6f}",
                "eq_cont_next": f"{eq_cont[-1]:.6f}",
            }
        )

    out_csv = out_dir / "equity_compare.csv"
    out_json = out_dir / "equity_compare_summary.json"

    fieldnames = list(rows_out[0].keys()) if rows_out else []
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows_out:
            w.writerow(r)

    s_none = _stats(eq_none)
    s_tier = _stats(eq_tier)
    s_cont = _stats(eq_cont)

    payload = {
        "updated_at": _now(),
        "run_id": run_id,
        "strategy": args.strategy,
        "topN": args.topN,
        "risk_on": args.risk_on,
        "risk_mid": args.risk_mid,
        "costs": {"commission": costs.commission, "sell_tax": costs.sell_tax, "slippage": costs.slippage},
        "bt_root": str(bt_root),
        "days": len(rows_out),
        "stats": {
            "none": {**s_none, "traded_days": traded_none, "no_trade_days": 0},
            "tier": {**s_tier, "traded_days": traded_tier, "no_trade_days": no_trade_tier},
            "continuous": {**s_cont, "traded_days": traded_cont, "no_trade_days": no_trade_cont},
        },
        "csv": str(out_csv),
    }

    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(f"[SAVED] {display_path(out_csv, args.out_dir)}")
    print(f"[SAVED] {display_path(out_json, args.out_dir)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())