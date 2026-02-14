# prepare_input.py
# TradingSystem data bridge (clean integration layer)
# - Reads latest TWSE JSON + TPEX JSON from inbound/
# - Normalizes to a stable schema -> daily_input.csv (UTF-8, no BOM)
# - Filters universe to common stocks only (exclude ETF/ETN/warrants/CB etc.)
# - Dynamic sanity checks based on parsed universe

from __future__ import annotations

import csv
import datetime as dt
import json
import re
from pathlib import Path
from typing import List, Dict, Optional, Tuple


BASE = Path(__file__).resolve().parents[2]  # back to C:\TradingSystem
DATA_DIR = BASE / "data"
INBOUND = DATA_DIR / "inbound"
PREPARED = DATA_DIR / "prepared"
DAILY_OUT = DATA_DIR / "daily_input.csv"


# -----------------------------
# Helpers
# -----------------------------
def _strip_commas_num(s: str) -> str:
    return (s or "").replace(",", "").strip()


def _to_float(s: str) -> float:
    s = _strip_commas_num(s)
    if s in ("", "-", "—"):
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def _to_int(s: str) -> int:
    s = _strip_commas_num(s)
    if s in ("", "-", "—"):
        return 0
    try:
        return int(float(s))
    except Exception:
        return 0


def _clean_code(code: str) -> str:
    c = (code or "").strip()
    c = re.sub(r"\.TWO$|\.TW$|\.T$", "", c, flags=re.IGNORECASE)
    return c.strip()


def _is_common_stock_code(code: str) -> bool:
    # Keep only 4-digit codes starting with 1-9 (1000~9999)
    # Excludes: ETF/ETN (often 0xxxx), warrants (often 6 digits), CB (often 5 digits / suffix), any letters
    c = (code or "").strip()
    return bool(re.fullmatch(r"[1-9]\d{3}", c))


def _find_latest_file(prefix: str) -> Path:
    files = sorted(INBOUND.glob(f"{prefix}_*.json"))
    if not files:
        raise FileNotFoundError(f"No inbound file found for prefix={prefix} under {INBOUND}")
    return files[-1]


def _load_json(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8", errors="replace"))


# -----------------------------
# Parsers
# -----------------------------
def parse_twse(mi_index: dict) -> List[Dict[str, object]]:
    """
    Support both:
    - rwd format: {"tables":[{"title":..., "fields":[...], "data":[...]} , ...]}
    - legacy format: {"fields9":[...], "data9":[...]}
    Picks the equity table by detecting required field names.
    """

    def parse_by_fields_data(fields: List[str], data: List[list]) -> List[Dict[str, object]]:
        idx = {str(name).strip(): i for i, name in enumerate(fields)}

        def g(row: list, key: str) -> str:
            i = idx.get(key, None)
            return row[i] if i is not None and i < len(row) else ""

        out: List[Dict[str, object]] = []
        for row in data:
            code = _clean_code(str(g(row, "證券代號")))
            name = str(g(row, "證券名稱")).strip()
            vol = _to_int(str(g(row, "成交股數")))
            tv = _to_float(str(g(row, "成交金額")))
            close = _to_float(str(g(row, "收盤價")))

            if not code:
                continue

            out.append(
                {
                    "code": code,
                    "name": name,
                    "market": "TWSE",
                    "close": float(close),
                    "volume": float(vol),
                    "trade_value": float(tv),
                    "turnover": 0.0,
                    "short_used_ratio": 0.0,
                    "margin_used_ratio": 0.0,
                }
            )
        return out

    # --- rwd format ---
    tables = mi_index.get("tables")
    if isinstance(tables, list) and tables:
        # Must-have fields for equities close quotes
        need = {"證券代號", "證券名稱", "收盤價", "成交股數", "成交金額"}

        for t in tables:
            if not isinstance(t, dict):
                continue
            fields = t.get("fields") or []
            data = t.get("data") or []
            if not isinstance(fields, list) or not isinstance(data, list) or not fields or not data:
                continue
            fieldset = {str(x).strip() for x in fields}
            if need.issubset(fieldset):
                return parse_by_fields_data([str(x).strip() for x in fields], data)

        # looser match
        loose_need = {"證券代號", "收盤價", "成交金額"}
        for t in tables:
            if not isinstance(t, dict):
                continue
            fields = t.get("fields") or []
            data = t.get("data") or []
            if not isinstance(fields, list) or not isinstance(data, list) or not fields or not data:
                continue
            fieldset = {str(x).strip() for x in fields}
            if loose_need.issubset(fieldset) and "證券名稱" in fieldset and "成交股數" in fieldset:
                return parse_by_fields_data([str(x).strip() for x in fields], data)

        return []

    # --- legacy format ---
    fields = mi_index.get("fields9") or []
    data = mi_index.get("data9") or []
    if isinstance(fields, list) and isinstance(data, list) and fields and data:
        return parse_by_fields_data([str(x).strip() for x in fields], data)

    return []


def parse_tpex(stk_quote: dict) -> List[Dict[str, object]]:
    """
    TPEX payload from our fetch step:
      - fields: column names
      - aaData: rows
    Uses column-name indexing to avoid shifts.
    """
    fields = stk_quote.get("fields") or []
    rows = stk_quote.get("aaData") or []

    if not isinstance(fields, list) or not isinstance(rows, list) or not fields or not rows:
        return []

    idx = {str(name).strip(): i for i, name in enumerate(fields)}

    def pick(*names: str) -> int:
        for n in names:
            if n in idx:
                return idx[n]
        return -1

    i_code = pick("代號", "證券代號", "股票代號")
    i_name = pick("名稱", "證券名稱", "股票名稱")
    i_close = pick("收盤", "收盤價")
    i_vol = pick("成交股數", "成交股數(股)", "成交量")
    i_tv = pick("成交金額(元)", "成交金額", "成交值")

    if i_code < 0 or i_close < 0:
        return []

    out: List[Dict[str, object]] = []
    for r in rows:
        if not isinstance(r, list):
            continue

        max_i = max(i_code, i_close, i_name if i_name >= 0 else 0, i_vol if i_vol >= 0 else 0, i_tv if i_tv >= 0 else 0)
        if len(r) <= max_i:
            continue

        code = _clean_code(str(r[i_code]))
        name = str(r[i_name]).strip() if i_name >= 0 else ""
        close = _to_float(str(r[i_close]))
        vol = _to_int(str(r[i_vol])) if i_vol >= 0 else 0
        tv = _to_float(str(r[i_tv])) if i_tv >= 0 else 0.0

        if not code:
            continue

        out.append(
            {
                "code": code,
                "name": name,
                "market": "TWO",
                "close": float(close),
                "volume": float(vol),
                "trade_value": float(tv),
                "turnover": 0.0,
                "short_used_ratio": 0.0,
                "margin_used_ratio": 0.0,
            }
        )

    return out


# -----------------------------
# Sanity (dynamic thresholds)
# -----------------------------
def sanity_dynamic(
    rows: List[Dict[str, object]],
    expected_twse: int,
    expected_two: int,
    min_ratio: float = 0.70,
    min_total_floor: int = 800,
) -> None:
    n = len(rows)

    twse_n = sum(1 for r in rows if str(r.get("market", "")).upper() == "TWSE")
    two_n = sum(1 for r in rows if str(r.get("market", "")).upper() == "TWO")

    min_twse = max(int(expected_twse * min_ratio), 200)
    min_two = max(int(expected_two * min_ratio), 50)
    min_total = max(int((expected_twse + expected_two) * min_ratio), min_total_floor)

    if n < min_total:
        raise RuntimeError(
            f"Sanity failed: total rows too few: {n} < {min_total} (expected~{expected_twse+expected_two}, ratio={min_ratio})"
        )

    if expected_twse > 0 and twse_n < min_twse:
        raise RuntimeError(
            f"Sanity failed: TWSE rows too few: {twse_n} < {min_twse} (expected~{expected_twse}, ratio={min_ratio})"
        )

    if expected_two > 0 and two_n < min_two:
        raise RuntimeError(
            f"Sanity failed: TWO rows too few: {two_n} < {min_two} (expected~{expected_two}, ratio={min_ratio})"
        )

    bad_close = sum(1 for r in rows if float(r.get("close", 0.0) or 0.0) <= 0.0)
    if bad_close / max(n, 1) > 0.10:
        raise RuntimeError(f"Sanity failed: too many non-positive close: {bad_close}/{n}")

    bad_code = sum(1 for r in rows if not str(r.get("code", "")).strip())
    if bad_code > 0:
        raise RuntimeError(f"Sanity failed: empty code count={bad_code}")

    bad_market = sum(1 for r in rows if str(r.get("market", "")).upper() not in ("TWSE", "TWO"))
    if bad_market > 0:
        raise RuntimeError(f"Sanity failed: unexpected market values count={bad_market}")


# -----------------------------
# Output writer
# -----------------------------
def write_daily(rows: List[Dict[str, object]], out_path: Path) -> None:
    # Always write UTF-8 without BOM to avoid \ufeffcode issues.
    cols = [
        "code",
        "name",
        "market",
        "close",
        "volume",
        "trade_value",
        "turnover",
        "short_used_ratio",
        "margin_used_ratio",
    ]
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in cols})
    tmp.replace(out_path)


# -----------------------------
# Main
# -----------------------------
def main() -> int:
    INBOUND.mkdir(parents=True, exist_ok=True)
    PREPARED.mkdir(parents=True, exist_ok=True)

    twse_path = _find_latest_file("twse_MI_INDEX")
    tpex_path = _find_latest_file("tpex_stk_quote")

    twse = _load_json(twse_path)
    tpex = _load_json(tpex_path)

    twse_rows = parse_twse(twse)
    tpex_rows = parse_tpex(tpex)
    print(f"[PARSE] twse_rows={len(twse_rows)} tpex_rows={len(tpex_rows)}")

    # expected universe for sanity should match our filter universe (common stocks only)
    exp_twse_stock = sum(1 for r in twse_rows if _is_common_stock_code(str(r.get("code", ""))))
    exp_tpex_stock = sum(1 for r in tpex_rows if _is_common_stock_code(str(r.get("code", ""))))
    print(f"[EXPECT] common_stocks twse={exp_twse_stock} tpex={exp_tpex_stock} total={exp_twse_stock + exp_tpex_stock}")


    rows = twse_rows + tpex_rows

    # de-dup by (market, code), keep last
    seen: Dict[Tuple[str, str], Dict[str, object]] = {}
    for r in rows:
        key = (str(r["market"]), str(r["code"]))
        seen[key] = r
    merged = list(seen.values())

    # filter to common stocks only
    before = len(merged)
    merged = [r for r in merged if _is_common_stock_code(str(r.get("code", "")))]
    after = len(merged)
    print(f"[FILTER] common_stocks_only: {before} -> {after}")

    # sanity: dynamic thresholds based on parsed universe (pre-filter)
    sanity_dynamic(
        merged,
        expected_twse=exp_twse_stock,
        expected_two=exp_tpex_stock,
        min_ratio=0.70,
        min_total_floor=800,
    )

    # dated snapshot for audit
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    snap = PREPARED / f"daily_input_{stamp}.csv"
    write_daily(merged, snap)

    # production path for oneclick
    write_daily(merged, DAILY_OUT)

    print(str(DAILY_OUT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
