# strategy_score.py
# TradingSystem v6.3.29-F4.7 (Stable) - unified lights logic
# - Unified lights logic (turnover / trade value / short squeeze)
# - Market-grouped trade value ranking (TWSE / TWO)
# - No dtype pollution (no LOW / emoji written into numeric columns)
#
# Expected columns (best effort; missing ones become NaN/0 safely):
#   code, name, market (TWSE/TWO) or exchange, close, volume, turnover,
#   trade_value (or amount), margin_used_ratio, short_used_ratio,
#   margin_change, short_change, etc.
#
# Output columns:
#   tv_rank_mkt, tv_pct_mkt, light_full, light_decision, light_top20
#
# Notes:
# - This file is designed to be defensive: it will not crash if some
#   columns are missing; it will just compute what it can.

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Tuple

import numpy as np
import pandas as pd


# -----------------------------
# Helpers
# -----------------------------
def _coalesce_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _ensure_numeric(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([np.nan] * len(df), index=df.index, dtype="float64")
    s = pd.to_numeric(df[col], errors="coerce")
    return s


def _safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    b2 = b.replace(0, np.nan)
    return a / b2


def _clip01(x: pd.Series) -> pd.Series:
    return x.clip(lower=0.0, upper=1.0)


def _as_market(df: pd.DataFrame) -> pd.Series:
    # prefer 'market', fallback to exchange-like columns; normalize to TWSE/TWO
    c = _coalesce_col(df, ["market", "exchange", "mkt", "board"])
    if c is None:
        return pd.Series(["UNK"] * len(df), index=df.index)
    s = df[c].astype(str).str.upper().str.strip()

    # normalize common variants
    s = s.replace(
        {
            "TSE": "TWSE",
            "TWSE": "TWSE",
            "上市": "TWSE",
            "OTC": "TWO",
            "TWO": "TWO",
            "上櫃": "TWO",
        }
    )
    # keep unknowns
    s = s.where(s.isin(["TWSE", "TWO"]), other="UNK")
    return s


# -----------------------------
# Lights scoring configuration
# -----------------------------
@dataclass(frozen=True)
class LightConfig:
    # Turnover thresholds (ratio)
    turnover_hi: float = 0.06
    turnover_mid: float = 0.03

    # Trade value percentile thresholds within market
    tv_pct_hi: float = 0.90
    tv_pct_mid: float = 0.70

    # Short squeeze proxies
    # - short_used_ratio: e.g. short_used / float_shares
    # - short_change: daily change (optional)
    short_used_hi: float = 0.09
    short_used_mid: float = 0.03

    # Margin usage (optional)
    margin_used_hi: float = 0.40
    margin_used_mid: float = 0.15

    # Top20 cutoff within each market group
    topn_each_market: int = 20


DEFAULT_CFG = LightConfig()


def compute_trade_value(df: pd.DataFrame) -> pd.Series:
    # Choose best available trade value column; fallback to close*volume
    tv_col = _coalesce_col(df, ["trade_value", "amount", "turnover_value", "成交金額", "成交值"])
    if tv_col is not None:
        tv = _ensure_numeric(df, tv_col)
        return tv

    close_col = _coalesce_col(df, ["close", "price", "收盤價"])
    vol_col = _coalesce_col(df, ["volume", "vol", "成交量"])
    close = _ensure_numeric(df, close_col) if close_col else pd.Series([np.nan] * len(df), index=df.index)
    vol = _ensure_numeric(df, vol_col) if vol_col else pd.Series([np.nan] * len(df), index=df.index)

    tv = close * vol
    return tv


def compute_turnover(df: pd.DataFrame) -> pd.Series:
    # If turnover exists, use it; else try volume / shares_float
    t_col = _coalesce_col(df, ["turnover", "turnover_ratio", "周轉率"])
    if t_col is not None:
        return _ensure_numeric(df, t_col)

    vol_col = _coalesce_col(df, ["volume", "vol", "成交量"])
    float_col = _coalesce_col(df, ["shares_float", "float_shares", "流通股數", "float"])
    vol = _ensure_numeric(df, vol_col) if vol_col else pd.Series([np.nan] * len(df), index=df.index)
    flt = _ensure_numeric(df, float_col) if float_col else pd.Series([np.nan] * len(df), index=df.index)

    return _safe_div(vol, flt)


def _pct_rank_within_group(s: pd.Series) -> pd.Series:
    # pct rank, NaN safe; higher = bigger
    return s.rank(pct=True, method="average")


def compute_market_trade_value_rank(df: pd.DataFrame, tv: pd.Series) -> Tuple[pd.Series, pd.Series]:
    mkt = _as_market(df)
    tv_rank = pd.Series(np.nan, index=df.index)
    tv_pct = pd.Series(np.nan, index=df.index)

    for g in ["TWSE", "TWO", "UNK"]:
        idx = (mkt == g)
        if idx.any():
            tv_g = tv[idx]
            # rank 1 = largest
            r = tv_g.rank(ascending=False, method="min")
            tv_rank.loc[idx] = r
            tv_pct.loc[idx] = _pct_rank_within_group(tv_g)
    return tv_rank, tv_pct


def _turnover_light(turnover: pd.Series, cfg: LightConfig) -> pd.Series:
    # 2 = high, 1 = mid, 0 = low/unknown
    out = pd.Series(0, index=turnover.index, dtype="int64")
    out = out.where(~(turnover >= cfg.turnover_mid), other=1)
    out = out.where(~(turnover >= cfg.turnover_hi), other=2)
    return out


def _trade_value_light(tv_pct_mkt: pd.Series, cfg: LightConfig) -> pd.Series:
    out = pd.Series(0, index=tv_pct_mkt.index, dtype="int64")
    out = out.where(~(tv_pct_mkt >= cfg.tv_pct_mid), other=1)
    out = out.where(~(tv_pct_mkt >= cfg.tv_pct_hi), other=2)
    return out


def _short_squeeze_light(df: pd.DataFrame, cfg: LightConfig) -> pd.Series:
    su_col = _coalesce_col(df, ["short_used_ratio", "short_use_ratio", "融券使用率"])
    su = _ensure_numeric(df, su_col) if su_col else pd.Series([np.nan] * len(df), index=df.index)

    out = pd.Series(0, index=df.index, dtype="int64")
    out = out.where(~(su >= cfg.short_used_mid), other=1)
    out = out.where(~(su >= cfg.short_used_hi), other=2)
    return out


def _margin_heat_light(df: pd.DataFrame, cfg: LightConfig) -> pd.Series:
    mu_col = _coalesce_col(df, ["margin_used_ratio", "margin_use_ratio", "融資使用率"])
    mu = _ensure_numeric(df, mu_col) if mu_col else pd.Series([np.nan] * len(df), index=df.index)

    out = pd.Series(0, index=df.index, dtype="int64")
    out = out.where(~(mu >= cfg.margin_used_mid), other=1)
    out = out.where(~(mu >= cfg.margin_used_hi), other=2)
    return out


def compute_lights(df: pd.DataFrame, cfg: LightConfig = DEFAULT_CFG) -> pd.DataFrame:
    """
    Adds:
      trade_value, turnover,
      tv_rank_mkt, tv_pct_mkt,
      light_full, light_decision, light_top20
    """
    df = df.copy()

    tv = compute_trade_value(df)
    df["trade_value"] = tv.astype("float64")

    turnover = compute_turnover(df)
    df["turnover"] = turnover.astype("float64")

    tv_rank_mkt, tv_pct_mkt = compute_market_trade_value_rank(df, tv)
    df["tv_rank_mkt"] = pd.to_numeric(tv_rank_mkt, errors="coerce")
    df["tv_pct_mkt"] = pd.to_numeric(tv_pct_mkt, errors="coerce")

    l_to = _turnover_light(turnover, cfg)
    l_tv = _trade_value_light(tv_pct_mkt, cfg)
    l_ss = _short_squeeze_light(df, cfg)
    l_mg = _margin_heat_light(df, cfg)

    # Full = composite signal count, normalized 0..4 then to 0..1
    full_raw = (l_to + l_tv + l_ss + l_mg).astype("int64")  # 0..8 but each is 0..2
    # We map to 0..4 in a stable way (cap at 4)
    full_score = (full_raw / 2.0).clip(0, 4)
    df["light_full"] = full_score.astype("float64")

    # Decision light: conservative binary-ish (>=2.0 considered "ON")
    df["light_decision"] = (df["light_full"] >= 2.0).astype("int64")

    # Top20 light: based on market-grouped trade value rank
    mkt = _as_market(df)
    top20 = pd.Series(0, index=df.index, dtype="int64")
    for g in ["TWSE", "TWO"]:
        idx = (mkt == g)
        if idx.any():
            top20.loc[idx] = (df.loc[idx, "tv_rank_mkt"] <= cfg.topn_each_market).astype("int64")
    df["light_top20"] = top20

    return df


__all__ = [
    "LightConfig",
    "DEFAULT_CFG",
    "compute_lights",
    "compute_trade_value",
    "compute_turnover",
    "compute_market_trade_value_rank",
]
