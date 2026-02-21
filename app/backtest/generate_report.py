from __future__ import annotations

import argparse
import csv
import html
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from app.ops.path_display import display_path


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


def _pct5(x: float) -> str:
    return f"{x * 100:.5f}%"


def _f5(x: float) -> str:
    return f"{x:.5f}"


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


def _fmt_money0(v: str) -> str:
    x = _as_float(v)
    if x != x or x == 0:
        return "-"
    return f"{x:,.0f}"


def _fmt_rank(v: str) -> str:
    x = _as_int(v)
    return "-" if x <= 0 else str(x)


def _fmt_num5(v: str) -> str:
    x = _as_float(v)
    if x != x:
        s = (v or "").strip()
        return "-" if s == "" else html.escape(s)
    return f"{x:.5f}"


def _fmt_bool_zh(v: str) -> str:
    s = (v or "").strip()
    if s in ("1", "TRUE", "True", "true", "Y", "y"):
        return "是"
    if s in ("0", "FALSE", "False", "false", "N", "n"):
        return "否"
    return "-" if s == "" else html.escape(s)


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate Traditional Chinese formal HTML report (with Top20 table).")
    ap.add_argument("--run_id", required=True)
    ap.add_argument("--out_dir", required=True)
    args = ap.parse_args()

    out_dir = Path(args.out_dir).resolve()
    reports_dir = out_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    eq_csv = out_dir / "equity_compare.csv"
    eq_html = out_dir / "equity_compare.html"
    summary_path = out_dir / "equity_compare_summary.json"

    daily_top20_path = out_dir / "daily_top20.csv"

    if not summary_path.exists():
        print(f"[REPORT] missing {display_path(summary_path, args.out_dir)}")
        return 1
    if not eq_csv.exists():
        print(f"[REPORT] missing {display_path(eq_csv, args.out_dir)}")
        return 1
    if not eq_html.exists():
        print(f"[REPORT] missing {display_path(eq_html, args.out_dir)} (run plot_equity_compare.py first)")
        return 1

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    stats_none = summary["stats"]["none"]
    stats_tier = summary["stats"]["tier"]
    stats_cont = summary["stats"]["continuous"]

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def perf_row(name: str, s: dict) -> str:
        return f"""
        <tr>
          <td class="left">{name}</td>
          <td>{_pct5(float(s["total_return"]))}</td>
          <td>{_f5(float(s["sharpe"]))}</td>
          <td>{_pct5(float(s["max_drawdown"]))}</td>
          <td>{int(s.get("traded_days", 0))}</td>
          <td>{int(s.get("no_trade_days", 0))}</td>
        </tr>
        """

    top20_html = ""
    if daily_top20_path.exists():
        rows = _read_csv_rows(daily_top20_path)[:20]
        if rows:
            trs = []
            for r in rows:
                code = html.escape((r.get("code", "") or "").strip())
                name = html.escape((r.get("name", "") or "").strip())
                market = html.escape((r.get("market", "") or "").strip())
                tv = _fmt_money0(r.get("trade_value", ""))
                rk = _fmt_rank(r.get("tv_rank_mkt", ""))
                pct_mkt = _fmt_num5(r.get("tv_pct_mkt", ""))
                turnover = _fmt_num5(r.get("turnover", ""))
                lf = _fmt_bool_zh(r.get("light_full", ""))
                ld = _fmt_bool_zh(r.get("light_decision", ""))
                lt = _fmt_bool_zh(r.get("light_top20", ""))

                trs.append(
                    f"""
                    <tr>
                      <td class="left mono">{code}</td>
                      <td class="left namewrap">{name}</td>
                      <td class="left">{market}</td>
                      <td>{tv}</td>
                      <td>{rk}</td>
                      <td>{pct_mkt}</td>
                      <td>{turnover}</td>
                      <td>{lf}</td>
                      <td>{ld}</td>
                      <td>{lt}</td>
                    </tr>
                    """
                )

            top20_html = f"""
            <div class="card">
              <h2>今日 Top20 清單（繁中顯示）</h2>
              <div class="note">
                來源：<code>{out_dir / "daily_top20.csv"}</code>
              </div>
              <table>
                <thead>
                  <tr>
                    <th style="text-align:left">代號</th>
                    <th style="text-align:left">名稱</th>
                    <th style="text-align:left">市場</th>
                    <th>成交金額</th>
                    <th>市場排名</th>
                    <th>市場百分位</th>
                    <th>週轉率</th>
                    <th>FULL燈</th>
                    <th>決策燈</th>
                    <th>Top20燈</th>
                  </tr>
                </thead>
                <tbody>
                  {''.join(trs)}
                </tbody>
              </table>
              <div class="note">註：表格數值欄位統一顯示至小數點後 5 位（成交金額/排名除外）。</div>
            </div>
            """
        else:
            top20_html = f"""
            <div class="card">
              <h2>今日 Top20 清單</h2>
              <div class="note">來源檔案存在但內容為空：<code>{out_dir / "daily_top20.csv"}</code></div>
            </div>
            """
    else:
        top20_html = f"""
        <div class="card">
          <h2>今日 Top20 清單</h2>
          <div class="note">尚未找到：<code>{out_dir / "daily_top20.csv"}</code>（請確認 oneclick 已產出）</div>
        </div>
        """

    html_out = f"""<!doctype html>
<html lang="zh-TW">
<head>
<meta charset="utf-8"/>
<title>策略回測正式報告 - {args.run_id}</title>
<style>
:root {{
  --bg:#f6f7f9; --card:#fff; --text:#111; --muted:#666; --line:#ddd; --h:#0b1320;
}}
body {{
  font-family: "Microsoft JhengHei","Noto Sans TC",sans-serif;
  background: var(--bg);
  color: var(--text);
  margin: 0;
}}
.container {{ max-width: 1020px; margin: 28px auto; padding: 0 18px; }}
.header {{
  background: var(--card);
  border: 1px solid var(--line);
  border-radius: 14px;
  padding: 18px 18px 10px 18px;
}}
h1 {{ margin: 0 0 6px 0; color: var(--h); font-size: 24px; }}
.meta {{ color: var(--muted); font-size: 13px; line-height: 1.6; }}
.card {{
  margin-top: 14px;
  background: var(--card);
  border: 1px solid var(--line);
  border-radius: 14px;
  padding: 16px 18px;
}}
h2 {{ margin: 0 0 10px 0; font-size: 18px; color: var(--h); }}
table {{ width:100%; border-collapse: collapse; font-size: 13px; table-layout: fixed; }}
th, td {{ border-bottom: 1px solid #eee; padding: 10px 8px; text-align: right; vertical-align: top; }}
th {{ background: #0b1320; color:#fff; font-weight: 600; }}
td.left {{ text-align:left; }}
.note {{ color: var(--muted); font-size: 12px; margin-top: 8px; }}
code {{ background:#f1f3f5; padding:2px 6px; border-radius: 6px; }}
.badge {{
  display:inline-block; padding: 2px 8px; border-radius: 999px; background:#eef2ff; color:#3142d0;
  font-size: 12px; margin-left: 8px;
}}
.mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace; }}
/* Name auto-wrap */
.namewrap {{
  white-space: normal;
  word-break: break-word;
  overflow-wrap: anywhere;
}}
</style>
</head>
<body>
<div class="container">

  <div class="header">
    <h1>策略回測正式報告 <span class="badge">繁體中文</span></h1>
    <div class="meta">
      執行編號：<b>{args.run_id}</b><br/>
      產生時間：{now}<br/>
      回測資料夾：<code>{html.escape(str(summary.get("bt_root","")))}</code><br/>
      回測有效天數：<b>{summary.get("days",0)}</b>
    </div>
  </div>

  <div class="card">
    <h2>績效摘要（含成本）</h2>
    <table>
      <thead>
        <tr>
          <th style="text-align:left">策略類型</th>
          <th>總報酬</th>
          <th>夏普比率</th>
          <th>最大回撤</th>
          <th>交易天數</th>
          <th>空手天數</th>
        </tr>
      </thead>
      <tbody>
        {perf_row("無風控", stats_none)}
        {perf_row("階梯風控", stats_tier)}
        {perf_row("連續曝險", stats_cont)}
      </tbody>
    </table>
    <div class="note">註：夏普比率為簡化年化估算（252 交易日），僅供策略比較。</div>
  </div>

  {top20_html}

  <div class="card">
    <h2>權益曲線</h2>
    <div class="note">
      檔案：<code>{out_dir / "equity_compare.html"}</code>（可直接用瀏覽器開啟）
    </div>
    <iframe src="../equity_compare.html" style="width:100%; height:520px; border:1px solid #eee; border-radius:12px; margin-top:10px"></iframe>
  </div>

</div>
</body>
</html>
"""

    out_html = reports_dir / f"report_{args.run_id}.html"
    out_html.write_text(html_out, encoding="utf-8")
    print(f"[REPORT] saved: {display_path(out_html, args.out_dir)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())