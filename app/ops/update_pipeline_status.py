from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _atomic_write_text(path: Path, text: str, retries: int = 3) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    last_err: Optional[Exception] = None
    for i in range(retries):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp.write_text(text, encoding="utf-8")
            tmp.replace(path)
            return
        except PermissionError as e:
            last_err = e
            time.sleep([0.2, 0.5, 1.0][min(i, 2)])
        except Exception as e:
            last_err = e
            time.sleep(0.2)
    if last_err:
        raise last_err


def _compute_overall(doc: Dict[str, Any]) -> str:
    stage_statuses = []
    for k, v in doc.items():
        if isinstance(v, dict) and "status" in v:
            s = str(v.get("status", "")).upper().strip()
            if s:
                stage_statuses.append(s)

    if any(s == "FAILED" for s in stage_statuses):
        return "FAILED"
    if any(s == "RUNNING" for s in stage_statuses):
        return "RUNNING"
    if stage_statuses:
        return "SUCCESS"
    return "RUNNING"


def _render_txt(doc: Dict[str, Any]) -> str:
    lines = []
    lines.append(f"updated_at: {doc.get('updated_at', '')}")
    lines.append(f"run_id: {doc.get('run_id', '')}")
    lines.append(f"overall_status: {doc.get('overall_status', '')}")
    lines.append("")

    order = ["fetch", "prepare", "quality", "oneclick", "report", "archive", "overall"]

    for stage in order:
        if stage not in doc:
            continue
        sdoc = doc.get(stage, {})
        if not isinstance(sdoc, dict):
            continue
        status = sdoc.get("status", "")
        at = sdoc.get("at", "")
        lines.append(f"[{stage}] status={status} at={at}")
        for k, v in sdoc.items():
            if k in ("status", "at"):
                continue
            lines.append(f"  {k}: {v}")
        lines.append("")

    for stage, sdoc in doc.items():
        if stage in ("updated_at", "run_id", "overall_status"):
            continue
        if stage in order:
            continue
        if not isinstance(sdoc, dict) or "status" not in sdoc:
            continue
        status = sdoc.get("status", "")
        at = sdoc.get("at", "")
        lines.append(f"[{stage}] status={status} at={at}")
        for k, v in sdoc.items():
            if k in ("status", "at"):
                continue
            lines.append(f"  {k}: {v}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True)
    ap.add_argument("--status", required=True)
    ap.add_argument("--run_id", required=True)
    ap.add_argument("--run_dir", required=True)
    ap.add_argument("--message", default="")
    ap.add_argument("--artifacts_json", default="")
    args = ap.parse_args()

    stage = str(args.stage).strip()
    status = str(args.status).upper().strip()
    run_id = str(args.run_id).strip()
    run_dir = Path(str(args.run_dir).strip())

    status_json = run_dir / "PIPELINE_STATUS.json"
    status_txt = run_dir / "PIPELINE_STATUS.txt"

    msg = str(args.message).strip()

    artifacts: Dict[str, Any] = {}
    if args.artifacts_json.strip():
        try:
            artifacts = json.loads(args.artifacts_json)
            if not isinstance(artifacts, dict):
                artifacts = {"artifacts": artifacts}
        except Exception:
            artifacts = {"artifacts_raw": args.artifacts_json}

    doc = _load_json(status_json)

    if stage.lower() == "reset":
        doc = {
            "updated_at": now_str(),
            "run_id": run_id,
            "overall_status": "RUNNING" if status == "RUNNING" else status,
        }
        _atomic_write_text(status_json, json.dumps(doc, ensure_ascii=False, indent=2) + "\n")
        _atomic_write_text(status_txt, _render_txt(doc))
        print(f"[PIPELINE_STATUS] reset run_id={run_id}")
        print(f"[PIPELINE_STATUS] json={status_json}")
        print(f"[PIPELINE_STATUS] txt ={status_txt}")
        return 0

    doc["run_id"] = run_id
    doc["updated_at"] = now_str()

    sdoc = doc.get(stage, {})
    if not isinstance(sdoc, dict):
        sdoc = {}

    sdoc["status"] = status
    sdoc["at"] = doc["updated_at"]

    # on SUCCESS, remove stale msg
    if status == "SUCCESS":
        sdoc.pop("msg", None)
    else:
        if msg:
            sdoc["msg"] = msg

    if artifacts:
        for k, v in artifacts.items():
            sdoc[k] = v

    doc[stage] = sdoc
    doc["overall_status"] = _compute_overall(doc)

    _atomic_write_text(status_json, json.dumps(doc, ensure_ascii=False, indent=2) + "\n")
    _atomic_write_text(status_txt, _render_txt(doc))

    print(f"[PIPELINE_STATUS] stage={stage} status={status} overall={doc['overall_status']}")
    print(f"[PIPELINE_STATUS] json={status_json}")
    print(f"[PIPELINE_STATUS] txt ={status_txt}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
