# daily_auto_run_final.py
# TradingSystem v6.3.29-F4.7 (Stable) - main pipeline
#
# Responsibilities:
# - Load daily universe (csv/parquet) -> compute lights via strategy_score.compute_lights
# - Export enriched dataset
# - Export Top20 via export_top20 (optional)
#
# This script is intentionally "pluggable": you can call run_pipeline() from other entrypoints.

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Optional

import pandas as pd

from strategy_score import compute_lights, DEFAULT_CFG
from export_top20 import export_top20


def _read_any(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    suffix = path.suffix.lower()
    if suffix in [".csv"]:
        return pd.read_csv(path)
    if suffix in [".parquet"]:
        return pd.read_parquet(path)
    if suffix in [".feather"]:
        return pd.read_feather(path)

    raise ValueError(f"Unsupported input format: {suffix} (use .csv/.parquet/.feather)")


def _write_any(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix.lower()
    if suffix in [".csv"]:
        df.to_csv(path, index=False, encoding="utf-8-sig")
        return
    if suffix in [".parquet"]:
        df.to_parquet(path, index=False)
        return
    if suffix in [".feather"]:
        df.to_feather(path)
        return
    raise ValueError(f"Unsupported output format: {suffix} (use .csv/.parquet/.feather)")


def run_pipeline(
    input_path: str,
    output_path: str,
    top20_output_path: Optional[str] = None,
    mode: str = "pre",
) -> None:
    """
    mode: pre / post (kept for compatibility with your oneclick_daily_run.py)
    """
    in_path = Path(input_path)
    out_path = Path(output_path)

    df = _read_any(in_path)

    # Compute lights
    enriched = compute_lights(df, DEFAULT_CFG)

    _write_any(enriched, out_path)

    # Export Top20 if requested
    if top20_output_path:
        export_top20(enriched, top20_output_path)


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="TradingSystem daily pipeline (stable core)")
    p.add_argument("--input", required=True, help="Input dataset path (.csv/.parquet/.feather)")
    p.add_argument("--output", required=True, help="Output enriched dataset path")
    p.add_argument("--top20", default="", help="Optional Top20 output path (.csv recommended)")
    p.add_argument("--mode", default="pre", choices=["pre", "post"], help="Run mode (compat)")
    return p


def main() -> None:
    args = build_arg_parser().parse_args()
    top20_path = args.top20.strip() or None
    run_pipeline(
        input_path=args.input,
        output_path=args.output,
        top20_output_path=top20_path,
        mode=args.mode,
    )


if __name__ == "__main__":
    main()
