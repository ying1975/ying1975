from __future__ import annotations

import runpy
from pathlib import Path

# Wrapper to keep backward-compatible path for oneclick_daily_run.py
TARGET = Path(__file__).resolve().parent / "app" / "core" / "daily_auto_run_final.py"
runpy.run_path(str(TARGET), run_name="__main__")
