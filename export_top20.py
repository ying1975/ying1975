# export_top20.py
# TradingSystem v6.3.29-F4.7 (Stable) - Top20 exporter
#
# Goal:
# - Output Top20 list with NO duplicated logic
# - Top20 is determined by light_top20 already computed in strategy_score.compute_lights
#
# Expected columns in enriched df:
#   code, name (optional), market, trade_value, tv_rank_mkt, light_top20, light_full, light_decision

from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd


def export_top20(enriched_df: pd.DataFrame, output_path: str) -> pd.DataFrame:
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df = enriched_df.copy()

    # Keep only top20 rows (light_top20 == 1)
    if "light_top20" in df.columns:
        df = df[df["light_top20"] == 1].copy()
    else:
        # fallback: if someone calls exporter without computing lights, do nothing harmful
        df = df.iloc[0:0].copy()

    # choose reasonable display columns if present
    cols_priority: List[str] = [
        "code",
        "name",
        "market",
        "trade_value",
        "tv_rank_mkt",
        "tv_pct_mkt",
        "turnover",
        "light_full",
        "light_decision",
        "light_top20",
    ]
    cols = [c for c in cols_priority if c in df.columns]

    # stable sort: market then rank
    sort_cols = [c for c in ["market", "tv_rank_mkt", "trade_value"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, ascending=[True] * len(sort_cols), kind="mergesort")

    df_out = df[cols].copy()
    df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
    return df_out


__all__ = ["export_top20"]
