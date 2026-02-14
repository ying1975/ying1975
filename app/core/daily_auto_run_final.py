# daily_auto_run_final.py
# TradingSystem v6.3.29-F4.7 (Stable) - main pipeline (with core fallback)
#
# Layer-2 fallback:
# - Primary run uses local core (strategy_score.py / export_top20.py)
# - If computation/export fails, automatically re-run using fallback_core core files
#   located at: <this_script_dir>/fallback_core/
#
# Interface unchanged:
#   python daily_auto_run_final.py --input data.csv --output enriched.csv --top20 top20.csv --mode pre

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import importlib.util


def _read_any(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix == ".feather":
        return pd.read_feather(path)

    raise ValueError(f"Unsupported input format: {suffix} (use .csv/.parquet/.feather)")


def _write_any(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df.to_csv(path, index=False, encoding="utf-8-sig")
        return
    if suffix == ".parquet":
        df.to_parquet(path, index=False)
        return
    if suffix == ".feather":
        df.to_feather(path)
        return
    raise ValueError(f"Unsupported output format: {suffix} (use .csv/.parquet/.feather)")


def _load_module_from_path(module_name: str, file_path: Path):
    import sys

    if not file_path.exists():
        raise FileNotFoundError(f"Missing core file: {file_path}")
    spec = importlib.util.spec_from_file_location(module_name, str(file_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module spec: {file_path}")

    mod = importlib.util.module_from_spec(spec)

    # ✅ CRITICAL: register into sys.modules BEFORE exec_module
    # Dataclasses (and other libs) may look up cls.__module__ in sys.modules.
    sys.modules[module_name] = mod

    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    return mod


def _run_with_core(
    core_dir: Path,
    df: pd.DataFrame,
    output_path: Path,
    top20_output_path: Optional[Path],
) -> None:
    """
    Run pipeline using core_dir/{strategy_score.py, export_top20.py}.
    This avoids sys.path pollution and guarantees we use the intended core version.
    """
    strategy_mod = _load_module_from_path(f"strategy_score__{core_dir.name}", core_dir / "strategy_score.py")
    export_mod = _load_module_from_path(f"export_top20__{core_dir.name}", core_dir / "export_top20.py")

    # Required functions / cfg
    compute_lights = getattr(strategy_mod, "compute_lights")
    default_cfg = getattr(strategy_mod, "DEFAULT_CFG")
    export_top20 = getattr(export_mod, "export_top20")

    enriched = compute_lights(df, default_cfg)
    _write_any(enriched, output_path)

    if top20_output_path:
        export_top20(enriched, str(top20_output_path))


def run_pipeline(
    input_path: str,
    output_path: str,
    top20_output_path: Optional[str] = None,
    mode: str = "pre",
) -> None:
    """
    mode: pre / post (kept for compatibility)
    """
    base_dir = Path(__file__).resolve().parent
    fallback_core_dir = base_dir / "fallback_core"

    in_path = Path(input_path).resolve()
    out_path = Path(output_path).resolve()
    top20_path = Path(top20_output_path).resolve() if top20_output_path else None

    df = _read_any(in_path)

    # Primary attempt: use local core (this folder)
    try:
        _run_with_core(base_dir, df, out_path, top20_path)
        return
    except Exception as e_primary:
        # Layer-2 fallback: use fallback_core
        if fallback_core_dir.exists():
            try:
                _run_with_core(fallback_core_dir, df, out_path, top20_path)
                return
            except Exception as e_fb:
                raise RuntimeError(
                    "Primary core failed, and fallback_core also failed.\n"
                    f"[PRIMARY ERROR] {type(e_primary).__name__}: {e_primary}\n"
                    f"[FALLBACK ERROR] {type(e_fb).__name__}: {e_fb}\n"
                    f"fallback_core_dir={fallback_core_dir}"
                ) from e_fb

        raise RuntimeError(
            "Primary core failed, and fallback_core directory not found.\n"
            f"[PRIMARY ERROR] {type(e_primary).__name__}: {e_primary}\n"
            f"Expected fallback_core at: {fallback_core_dir}"
        ) from e_primary


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="TradingSystem daily pipeline (stable core, with fallback)")
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
