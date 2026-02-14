from __future__ import annotations

import datetime as dt
import json
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Tuple


BASE = Path(__file__).resolve().parents[2]
OUT_DIR = BASE / "data" / "inbound"


def _yyyymmdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


def _http_get_json(url: str, timeout: int = 30) -> dict:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/plain,*/*",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8", errors="replace"))


def fetch_for_date_rwd(d: dt.date) -> dict:
    # Newer/robust path: returns JSON with keys like: tables, params, stat, date
    # Example: /rwd/zh/afterTrading/MI_INDEX?date=YYYYMMDD&type=ALLBUT0999&response=json
    qs = urllib.parse.urlencode(
        {
            "date": _yyyymmdd(d),
            "type": "ALLBUT0999",
            "response": "json",
            "_": str(int(time.time() * 1000)),
        }
    )
    url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?{qs}"
    return _http_get_json(url)


def fetch_for_date_exchange(d: dt.date) -> dict:
    # Legacy path: returns JSON with keys like fields9/data9 (sometimes)
    qs = urllib.parse.urlencode(
        {
            "response": "json",
            "date": _yyyymmdd(d),
            "type": "ALLBUT0999",
            "_": str(int(time.time() * 1000)),
        }
    )
    url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?{qs}"
    return _http_get_json(url)


def _looks_ok(payload: dict) -> bool:
    stat = str(payload.get("stat", "")).strip()
    ok = ("OK" in stat.upper()) or ("成功" in stat) or (stat == "")

    # rwd format: tables is a list of dicts; each table has data list
    if isinstance(payload.get("tables"), list):
        for t in payload["tables"]:
            if isinstance(t, dict) and isinstance(t.get("data"), list) and len(t["data"]) > 0:
                return ok

    # legacy format: data* lists exist
    has_any_data = any(
        isinstance(v, list) and len(v) > 0
        for k, v in payload.items()
        if isinstance(k, str) and k.startswith("data")
    )
    return ok and has_any_data


def find_latest(max_lookback_days: int = 14) -> Tuple[dt.date, dict, str]:
    today = dt.date.today()

    for i in range(max_lookback_days + 1):
        d = today - dt.timedelta(days=i)

        # Try rwd first
        try:
            data = fetch_for_date_rwd(d)
            if _looks_ok(data):
                return d, data, "rwd"
        except Exception:
            pass

        # Fallback to legacy
        try:
            data = fetch_for_date_exchange(d)
            if _looks_ok(data):
                return d, data, "exchangeReport"
        except Exception:
            pass

    raise RuntimeError(f"TWSE fetch failed: no valid data within last {max_lookback_days} days")


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    d, data, source = find_latest()
    out = OUT_DIR / f"twse_MI_INDEX_{_yyyymmdd(d)}.json"
    out.write_text(json.dumps({"_source": source, **data}, ensure_ascii=False) + "\n", encoding="utf-8")
    print(str(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
