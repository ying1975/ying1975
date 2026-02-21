from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

from reportlab.lib.pagesizes import A4, landscape
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.utils import simpleSplit

from app.ops.path_display import display_path


def _try_register_ttf(font_name: str, paths: list[str]) -> bool:
    for p in paths:
        fp = Path(p)
        if fp.exists():
            try:
                pdfmetrics.registerFont(TTFont(font_name, str(fp)))
                return True
            except Exception:
                continue
    return False


def _register_chinese_font() -> str:
    if _try_register_ttf(
        "TC_FONT",
        [
            r"C:\Windows\Fonts\msjh.ttf",
            r"C:\Windows\Fonts\msjhbd.ttf",
            r"C:\Windows\Fonts\kaiu.ttf",
            r"C:\TradingSystem\assets\fonts\NotoSansTC-Regular.otf",
            r"C:\TradingSystem\assets\fonts\NotoSansTC-Regular.ttf",
        ],
    ):
        return "TC_FONT"

    pdfmetrics.registerFont(UnicodeCIDFont("MSung-Light"))
    return "MSung-Light"


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        rows: List[Dict[str, str]] = []
        for row in r:
            rr: Dict[str, str] = {}
            for k, v in row.items():
                if k is None:
                    continue
                rr[k.lstrip("\ufeff")] = v if v is not None else ""
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


def _pct5(v: float) -> str:
    return f"{v * 100:.5f}%"


def _f5(v: float) -> str:
    return f"{v:.5f}"


def _fmt_money0(v: str) -> str:
    x = _as_float(v)
    if x != x or x == 0:
        return "-"
    return f"{x:,.0f}"


def _fmt_num5(v: str) -> str:
    x = _as_float(v)
    if x != x:
        s = (v or "").strip()
        return "-" if s == "" else s
    return f"{x:.5f}"


def _fmt_rank(v: str) -> str:
    x = _as_int(v)
    return "-" if x <= 0 else str(x)


def _fmt_bool_zh(v: str) -> str:
    s = (v or "").strip()
    if s in ("1", "TRUE", "True", "true", "Y", "y"):
        return "是"
    if s in ("0", "FALSE", "False", "false", "N", "n"):
        return "否"
    return "-" if s == "" else s


def _draw_text(c: canvas.Canvas, x: float, y: float, text: str, font: str, size: int) -> None:
    c.setFont(font, size)
    c.drawString(x, y, text)


def _draw_right(c: canvas.Canvas, x_right: float, y: float, text: str, font: str, size: int) -> None:
    c.setFont(font, size)
    c.drawRightString(x_right, y, text)


def _new_page_landscape(c: canvas.Canvas, font: str, title: str) -> Tuple[float, float, float]:
    c.showPage()
    W, H = landscape(A4)
    c.setFont(font, 14)
    c.drawString(40, H - 40, title)
    return W, H, H - 70.0


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate Traditional Chinese formal PDF report (Top20 10 columns, name wrap).")
    ap.add_argument("--run_id", required=True)
    ap.add_argument("--out_dir", required=True)
    args = ap.parse_args()

    out_dir = Path(args.out_dir).resolve()
    reports_dir = out_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    summary_path = out_dir / "equity_compare_summary.json"
    if not summary_path.exists():
        print(f"[PDF] missing {display_path(summary_path, args.out_dir)}")
        return 1

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    stats_none = summary["stats"]["none"]
    stats_tier = summary["stats"]["tier"]
    stats_cont = summary["stats"]["continuous"]

    top20_path = out_dir / "daily_top20.csv"
    top20_rows = _read_csv_rows(top20_path)[:20] if top20_path.exists() else []

    font = _register_chinese_font()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    out_pdf = reports_dir / f"report_{args.run_id}.pdf"
    c = canvas.Canvas(str(out_pdf), pagesize=landscape(A4))
    W, H = landscape(A4)

    MARGIN_X = 40.0
    y = H - 50.0

    _draw_text(c, MARGIN_X, y, "策略回測正式報告（繁體中文）", font, 18)
    y -= 22
    _draw_text(c, MARGIN_X, y, f"執行編號：{args.run_id}", font, 11)
    y -= 16
    _draw_text(c, MARGIN_X, y, f"產生時間：{now}", font, 11)
    y -= 16
    _draw_text(c, MARGIN_X, y, f"回測資料夾：{summary.get('bt_root','')}", font, 9)
    y -= 16
    _draw_text(c, MARGIN_X, y, f"回測有效天數：{summary.get('days',0)}", font, 11)
    y -= 12

    _draw_text(c, MARGIN_X, y, "績效摘要（含成本）", font, 13)
    y -= 18
    _draw_text(
        c,
        MARGIN_X,
        y,
        f"無風控：總報酬 {_pct5(float(stats_none['total_return']))}｜夏普比率 {_f5(float(stats_none['sharpe']))}｜最大回撤 {_pct5(float(stats_none['max_drawdown']))}",
        font,
        10,
    )
    y -= 14
    _draw_text(
        c,
        MARGIN_X,
        y,
        f"階梯風控：總報酬 {_pct5(float(stats_tier['total_return']))}｜夏普比率 {_f5(float(stats_tier['sharpe']))}｜最大回撤 {_pct5(float(stats_tier['max_drawdown']))}",
        font,
        10,
    )
    y -= 14
    _draw_text(
        c,
        MARGIN_X,
        y,
        f"連續曝險：總報酬 {_pct5(float(stats_cont['total_return']))}｜夏普比率 {_f5(float(stats_cont['sharpe']))}｜最大回撤 {_pct5(float(stats_cont['max_drawdown']))}",
        font,
        10,
    )
    y -= 18

    _draw_text(c, MARGIN_X, y, "今日 Top20 清單（10 欄）", font, 13)
    y -= 14

    if not top20_rows:
        _draw_text(c, MARGIN_X, y, "未找到 daily_top20.csv 或內容為空。", font, 11)
        y -= 16
    else:
        cols = [
            ("code", "代號", 58, "L"),
            ("name", "名稱", 200, "L"),
            ("market", "市場", 46, "L"),
            ("trade_value", "成交金額", 78, "R"),
            ("tv_rank_mkt", "市場排名", 62, "R"),
            ("tv_pct_mkt", "市場百分位", 74, "R"),
            ("turnover", "週轉率", 70, "R"),
            ("light_full", "FULL燈", 46, "R"),
            ("light_decision", "決策燈", 46, "R"),
            ("light_top20", "Top20燈", 50, "R"),
        ]

        table_x = MARGIN_X
        header_font_size = 9
        cell_font_size = 9
        line_gap = 2.0
        line_h = cell_font_size + line_gap
        min_row_h = 14.0
        table_w = sum(w for _, _, w, _ in cols)

        def draw_header(y0: float) -> float:
            c.setLineWidth(1)
            c.line(table_x, y0 - 3, table_x + table_w, y0 - 3)
            x = table_x
            for _, name, w, align in cols:
                if align == "L":
                    _draw_text(c, x + 2, y0, name, font, header_font_size)
                else:
                    _draw_right(c, x + w - 2, y0, name, font, header_font_size)
                x += w
            return y0 - min_row_h

        y = draw_header(y)

        for r in top20_rows:
            vmap: Dict[str, str] = {
                "code": (r.get("code", "") or "").strip(),
                "name": (r.get("name", "") or "").strip(),
                "market": (r.get("market", "") or "").strip(),
                "trade_value": _fmt_money0(r.get("trade_value", "")),
                "tv_rank_mkt": _fmt_rank(r.get("tv_rank_mkt", "")),
                "tv_pct_mkt": _fmt_num5(r.get("tv_pct_mkt", "")),
                "turnover": _fmt_num5(r.get("turnover", "")),
                "light_full": _fmt_bool_zh(r.get("light_full", "")),
                "light_decision": _fmt_bool_zh(r.get("light_decision", "")),
                "light_top20": _fmt_bool_zh(r.get("light_top20", "")),
            }

            name_col_w = next(w for k, _, w, _ in cols if k == "name")
            name_lines = simpleSplit(vmap["name"] or "-", font, cell_font_size, name_col_w - 4) or ["-"]
            row_h = max(min_row_h, len(name_lines) * line_h)

            if y - row_h < 70:
                W, H, y = _new_page_landscape(c, font, f"今日 Top20 清單（續） - {args.run_id}")
                y = draw_header(y)

            c.setLineWidth(0.5)
            c.line(table_x, y - 3, table_x + table_w, y - 3)

            x = table_x
            for key, _, w, align in cols:
                if key == "name":
                    yy = y
                    for ln in name_lines:
                        _draw_text(c, x + 2, yy, ln, font, cell_font_size)
                        yy -= line_h
                else:
                    val = vmap.get(key, "-") or "-"
                    if align == "L":
                        _draw_text(c, x + 2, y, val, font, cell_font_size)
                    else:
                        _draw_right(c, x + w - 2, y, val, font, cell_font_size)
                x += w

            y -= row_h

    if y < 90:
        c.showPage()
        W, H = landscape(A4)
        y = H - 60

    y -= 8
    _draw_text(c, MARGIN_X, y, "輸出檔案", font, 13)
    y -= 16
    _draw_text(c, MARGIN_X + 10, y, f"daily_top20.csv：{out_dir / 'daily_top20.csv'}", font, 9)
    y -= 12
    _draw_text(c, MARGIN_X + 10, y, f"equity_compare.html：{out_dir / 'equity_compare.html'}", font, 9)
    y -= 12
    _draw_text(c, MARGIN_X + 10, y, f"report HTML：{reports_dir / f'report_{args.run_id}.html'}", font, 9)
    y -= 12
    _draw_text(c, MARGIN_X + 10, y, f"report PDF：{out_pdf}", font, 9)

    c.save()
    print(f"[PDF] saved: {display_path(out_pdf, args.out_dir)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())