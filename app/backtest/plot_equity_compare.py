from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import List, Tuple

from app.ops.path_display import display_path


def _read_csv(path: Path) -> List[dict]:
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        return list(r)


def _to_float(x: str) -> float:
    try:
        return float(str(x).strip())
    except Exception:
        return 0.0


def _minmax(vals: List[float]) -> Tuple[float, float]:
    if not vals:
        return 0.0, 1.0
    mn = min(vals)
    mx = max(vals)
    if mx <= mn:
        return mn, mn + 1.0
    return mn, mx


def _polyline(points: List[Tuple[float, float]]) -> str:
    return " ".join(f"{x:.2f},{y:.2f}" for x, y in points)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run_id", required=True)
    ap.add_argument("--out_dir", required=True)
    args = ap.parse_args()

    out_dir = Path(args.out_dir).resolve()
    in_csv = out_dir / "equity_compare.csv"
    out_html = out_dir / "equity_compare.html"

    if not in_csv.exists():
        print(f"[PLOT] missing input: {display_path(in_csv, args.out_dir)}")
        return 1

    rows = _read_csv(in_csv)
    if not rows:
        print("[PLOT] empty input csv")
        return 1

    eq_none = [_to_float(r.get("eq_none_next", "0")) for r in rows]
    eq_tier = [_to_float(r.get("eq_tier_next", "0")) for r in rows]
    eq_cont = [_to_float(r.get("eq_cont_next", "0")) for r in rows]
    dates = [r.get("date", "") for r in rows]

    mn, mx = _minmax(eq_none + eq_tier + eq_cont)

    # SVG canvas
    W, H = 1000.0, 420.0
    PAD_L, PAD_R, PAD_T, PAD_B = 60.0, 20.0, 20.0, 50.0
    PW = W - PAD_L - PAD_R
    PH = H - PAD_T - PAD_B

    def sx(i: int) -> float:
        n = max(1, len(rows) - 1)
        return PAD_L + (i / n) * PW

    def sy(v: float) -> float:
        # invert y
        return PAD_T + (1.0 - (v - mn) / (mx - mn)) * PH

    pts_none = [(sx(i), sy(v)) for i, v in enumerate(eq_none)]
    pts_tier = [(sx(i), sy(v)) for i, v in enumerate(eq_tier)]
    pts_cont = [(sx(i), sy(v)) for i, v in enumerate(eq_cont)]

    html = f"""<!doctype html>
<html lang="zh-TW">
<head>
<meta charset="utf-8"/>
<title>權益曲線比較 - {args.run_id}</title>
<style>
body {{ font-family: "Microsoft JhengHei","Noto Sans TC",sans-serif; margin: 24px; }}
h1 {{ margin: 0 0 6px 0; }}
.meta {{ color: #555; margin-bottom: 16px; }}
.card {{ background: #fff; border: 1px solid #ddd; border-radius: 12px; padding: 16px; }}
.legend span {{ display: inline-block; margin-right: 14px; }}
.sw {{ width: 12px; height: 12px; display: inline-block; margin-right: 6px; border-radius: 2px; vertical-align: -2px; }}
.small {{ color:#666; font-size: 12px; }}
</style>
</head>
<body>
<h1>權益曲線比較</h1>
<div class="meta">執行編號：<b>{args.run_id}</b>　|　資料：{display_path(in_csv, args.out_dir)}</div>

<div class="card">
<div class="legend">
  <span><i class="sw" style="background:#1f77b4"></i>無風控</span>
  <span><i class="sw" style="background:#ff7f0e"></i>階梯風控</span>
  <span><i class="sw" style="background:#2ca02c"></i>連續曝險</span>
</div>

<svg width="{int(W)}" height="{int(H)}" viewBox="0 0 {W} {H}" style="margin-top:12px">
  <rect x="0" y="0" width="{W}" height="{H}" fill="#ffffff" />
  <line x1="{PAD_L}" y1="{PAD_T}" x2="{PAD_L}" y2="{H-PAD_B}" stroke="#999" />
  <line x1="{PAD_L}" y1="{H-PAD_B}" x2="{W-PAD_R}" y2="{H-PAD_B}" stroke="#999" />

  <polyline fill="none" stroke="#1f77b4" stroke-width="2" points="{_polyline(pts_none)}" />
  <polyline fill="none" stroke="#ff7f0e" stroke-width="2" points="{_polyline(pts_tier)}" />
  <polyline fill="none" stroke="#2ca02c" stroke-width="2" points="{_polyline(pts_cont)}" />

  <text x="{PAD_L}" y="{H-18}" font-size="12" fill="#555">日期（{dates[0]} → {dates[-1]}）</text>
  <text x="10" y="{PAD_T+12}" font-size="12" fill="#555">權益</text>
  <text x="10" y="{H-PAD_B}" font-size="12" fill="#555">{mn:,.0f}</text>
  <text x="10" y="{PAD_T+16}" font-size="12" fill="#555">{mx:,.0f}</text>
</svg>

<div class="small">提示：此圖為離線 SVG 輸出，無需網路或外部套件。</div>
</div>
</body>
</html>
"""

    out_html.write_text(html, encoding="utf-8")
    print(f"[PLOT] saved: {display_path(out_html, args.out_dir)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())