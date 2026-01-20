from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..utils.json_utils import write_json


def _read_json_or_none(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def review_map_by_instance_key(review_obj: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Return a mapping from instance_key -> review row object.
    """
    if not review_obj:
        return {}
    rows = review_obj.get("rows") or []
    return {str(row.get("instance_key")): row for row in rows if row.get("instance_key")}


def _minimal_review_from_instances(instances_path: Path) -> Dict[str, Any]:
    try:
        data = json.loads(instances_path.read_text(encoding="utf-8"))
        rows: List[Dict[str, Any]] = []
        for inst in data.get("instances") or []:
            rows.append(
                {
                    "instance_key": inst.get("instance_key"),
                    "predicted": inst.get("state"),
                    "candidate_client": inst.get("candidate_client"),
                    "candidate_process": inst.get("candidate_process"),
                    "candidate_role": inst.get("candidate_role"),
                    "evidence_message_ids": [e.get("message_id") for e in (inst.get("evidence") or []) if e.get("message_id")],
                    "human_label": None,
                    "human_notes": None,
                }
            )
        return {"rows": rows}
    except Exception:
        return {"rows": []}


def load_or_init_review(run_dir: Path, review_template: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Load `review.json` if present; else return template content if provided; else create a minimal structure from instances.
    """
    run_dir = Path(run_dir)
    review_path = run_dir / "review.json"
    if review_path.exists():
        obj = _read_json_or_none(review_path)
        if obj is not None:
            return obj
    if review_template:
        # Return a copy to avoid accidental mutation of cached template
        return json.loads(json.dumps(review_template))
    # Fallback: build minimal from instances
    return _minimal_review_from_instances(run_dir / "instances.json")


def save_review(run_dir: Path, review_obj: Dict[str, Any]) -> None:
    """
    Atomic write of review.json.
    """
    run_dir = Path(run_dir)
    write_json(run_dir / "review.json", review_obj)
