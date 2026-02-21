# C:\TradingSystem\app\ops\path_display.py
from __future__ import annotations

from pathlib import Path


def display_path(p: str | Path, display_root: str | Path) -> str:
    """
    Return a stable display path string based on the user-provided display_root
    (typically args.out_dir). This avoids showing resolved/junction physical paths.

    - p: actual path (Path or string)
    - display_root: original out_dir string/path provided by user

    If p is under display_root (by parts comparison), show it as display_root/relative.
    Otherwise, return p as-is (string).
    """
    p = Path(p)
    dr = Path(display_root)

    # Normalize only for comparison (no resolve); use parts-based prefix check
    p_parts = p.parts
    dr_parts = dr.parts

    if len(p_parts) >= len(dr_parts) and p_parts[: len(dr_parts)] == dr_parts:
        rel = Path(*p_parts[len(dr_parts) :])
        return str(dr / rel)

    # If p is absolute somewhere else (e.g., resolved physical path),
    # try to map by filename/structure when common suffix exists.
    # This is best-effort and still "display-only".
    try:
        # If filename exists, just return filename under display_root when appropriate.
        # (Safe fallback; better than showing C:\Users\...)
        if p.name:
            return str(dr / p.name)
    except Exception:
        pass

    return str(p)
