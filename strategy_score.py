from __future__ import annotations

import runpy
from pathlib import Path

TARGET = Path(__file__).resolve().parent / "app" / "core" / "strategy_score.py"
runpy.run_path(str(TARGET), run_name="__main__")
