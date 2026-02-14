from __future__ import annotations

import csv
import datetime as dt
import json
import re
import urllib.request
from pathlib import Path
from typing import List, Dict, Any, Tuple


BASE = Path(__file__).resolve().parents[2]
OUT_DIR = BASE / "data" / "inbound"


# Official CSV open-data出口：不帶日期，回傳最新一期(最近交易日)的每日收盤行情
TPEX_CSV_URL = "https://www.tpex.org.tw/web/stock/aftertrading/DAILY_CLOSE_quotes/stk_quote_result.php?l=zh-tw&o=data"


def _http_get_text(url: str, timeout: int = 30) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/csv,text/plain,*/*",
            "Referer": "https://www.tpex.org.tw/",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    # 官方宣稱 UTF-8；若遇到奇怪字元，用 replace 不中斷
    return raw.decode("utf-8", errors="replace")


def _extract_report_date(text: str) -> str:
    # 常見會出現：資料日期:115/02/11 或 reportDate:115/02/11
    m = re.search(r"(資料日期|reportDate)\s*[:：]\s*([0-9]{3}/[0-9]{2}/[0-9]{2})", text)
    if m:
        return m.group(2)
    return ""


def _roc_to_ad_yyyymmdd(roc: str) -> str:
    # 115/02/11 -> 20260211
    try:
        y, m, d = roc.split("/")
        ad_y = int(y) + 1911
        return f"{ad_y:04d}{int(m):02d}{int(d):02d}"
    except Exception:
        return dt.date.today().strftime("%Y%m%d")


def _parse_csv_to_payload(text: str) -> Tuple[str, Dict[str, Any]]:
    lines = [ln for ln in text.replace("\r", "").split("\n") if ln.strip() != ""]
    report_roc = _extract_report_date(text)

    # 找到 header 行：包含 代號/名稱/收盤 (這三個最穩)
    header_idx = -1
    header: List[str] = []
    for i, ln in enumerate(lines[:50]):
        row = next(csv.reader([ln]))
        row = [c.strip() for c in row]
        if ("代號" in row) and ("名稱" in row) and ("收盤" in row):
            header_idx = i
            header = row
            break

    if header_idx < 0 or not header:
        raise RuntimeError("TPEX CSV parse failed: cannot locate header row (代號/名稱/收盤)")

    data_rows: List[List[str]] = []
    for ln in lines[header_idx + 1 :]:
        row = next(csv.reader([ln]))
        row = [c.strip() for c in row]
        # 有些最後會有註記或空白列，長度太短就跳過
        if len(row) < 5:
            continue
        # 有些列第一欄可能是空
        if not row[0]:
            continue
        data_rows.append(row)

    if len(data_rows) == 0:
        raise RuntimeError("TPEX CSV parse failed: aaData is empty")

    payload: Dict[str, Any] = {
        "reportDate": report_roc,
        "fields": header,
        "aaData": data_rows,
        "_fetch_url": TPEX_CSV_URL,
        "_format": "csv_open_data",
    }

    ad = _roc_to_ad_yyyymmdd(report_roc) if report_roc else dt.date.today().strftime("%Y%m%d")
    return ad, payload


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    text = _http_get_text(TPEX_CSV_URL)
    yyyymmdd, payload = _parse_csv_to_payload(text)

    out = OUT_DIR / f"tpex_stk_quote_{yyyymmdd}.json"
    out.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")
    print(str(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
