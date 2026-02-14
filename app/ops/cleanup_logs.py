from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path


BASE = Path(__file__).resolve().parent
LOGS = BASE / "logs"
KEEP_DAYS = 30


def main() -> int:
    if not LOGS.exists():
        return 0

    cutoff = dt.datetime.now() - dt.timedelta(days=KEEP_DAYS)
    deleted = 0

    for p in LOGS.glob("*"):
        try:
            if not p.is_file():
                continue
            mtime = dt.datetime.fromtimestamp(p.stat().st_mtime)
            if mtime < cutoff:
                p.unlink()
                deleted += 1
        except Exception:
            # best-effort
            continue

    # Optional: write a tiny marker
    marker = BASE / "cleanup_last.txt"
    marker.write_text(
        f"{dt.datetime.now().isoformat(timespec='seconds')} deleted={deleted} keep_days={KEEP_DAYS}\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
