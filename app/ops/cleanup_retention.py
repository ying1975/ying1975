from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path


def _load_cfg(base: Path) -> dict:
    cfg = base / "app" / "config" / "config.prod.json"
    return json.loads(cfg.read_text(encoding="utf-8"))


def _cleanup_dir(dir_path: Path, keep_days: int) -> int:
    if not dir_path.exists():
        return 0
    cutoff = dt.datetime.now() - dt.timedelta(days=keep_days)
    deleted = 0
    for p in dir_path.glob("*"):
        try:
            if not p.is_file():
                continue
            mtime = dt.datetime.fromtimestamp(p.stat().st_mtime)
            if mtime < cutoff:
                p.unlink()
                deleted += 1
        except Exception:
            pass
    return deleted


def main() -> int:
    base = Path(r"C:\TradingSystem")
    cfg = _load_cfg(base)

    logs_dir = Path(cfg["paths"]["logs_dir"])
    inbound_dir = Path(cfg["paths"]["inbound_dir"])
    prepared_dir = Path(cfg["paths"]["prepared_dir"])

    keep_logs = int(cfg["retention"]["logs_days"])
    keep_inbound = int(cfg["retention"]["inbound_days"])
    keep_prepared = int(cfg["retention"]["prepared_days"])

    d1 = _cleanup_dir(logs_dir, keep_logs)
    d2 = _cleanup_dir(inbound_dir, keep_inbound)
    d3 = _cleanup_dir(prepared_dir, keep_prepared)

    marker = base / "data" / "retention_last.txt"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(
        f"{dt.datetime.now().isoformat(timespec='seconds')} "
        f"deleted_logs={d1} deleted_inbound={d2} deleted_prepared={d3}\n",
        encoding="utf-8",
    )

    print(f"[RETENTION] logs(-{keep_logs}d)={d1}, inbound(-{keep_inbound}d)={d2}, prepared(-{keep_prepared}d)={d3}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
