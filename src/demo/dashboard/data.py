from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import streamlit as st
except Exception:  # streamlit not available in some contexts (tests)
    st = None  # type: ignore

from ..utils.json_utils import read_jsonl
from ..catalog.loader import load_unified_catalog


def _cache_data(func):
    """
    Wrapper to use st.cache_data when available; otherwise no-op.
    """
    if st is not None and hasattr(st, "cache_data"):
        return st.cache_data(show_spinner=False)(func)
    return func


def _parse_iso(ts: str | None) -> Optional[datetime]:
    if not ts:
        return None
    try:
        # Try strict fromisoformat; handle 'Z' suffix
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.fromisoformat(ts)
    except Exception:
        return None


@_cache_data
def list_runs(runs_dir: Path) -> List[str]:
    """
    Return run IDs sorted by creation time (desc). Uses run_meta.json created_at when available.
    """
    runs_dir = Path(runs_dir)
    if not runs_dir.exists():
        return []
    run_ids: List[Tuple[str, datetime]] = []
    for child in runs_dir.iterdir():
        if not child.is_dir():
            continue
        run_id = child.name
        meta_path = child / "run_meta.json"
        created_at: Optional[datetime] = None
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                created_at = _parse_iso(meta.get("created_at"))
            except Exception:
                created_at = None
        if created_at is None:
            # fallback to directory mtime
            created_at = datetime.fromtimestamp(child.stat().st_mtime)
        run_ids.append((run_id, created_at))
    run_ids.sort(key=lambda t: t[1], reverse=True)
    return [rid for rid, _ in run_ids]


@dataclass
class RunData:
    run_id: str
    run_dir: Path
    run_meta: Dict[str, Any]
    instances: Dict[str, Any]
    timeline: Dict[str, Any]
    eval_report: Optional[Dict[str, Any]]
    review: Optional[Dict[str, Any]]
    review_template: Optional[Dict[str, Any]]
    normalized_messages_path: Optional[Path]


def _read_json_file(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise RuntimeError(f"Failed reading JSON: {path}: {e}") from e


@_cache_data
def load_run(run_dir: Path, cache_bust: Optional[int] = None) -> RunData:
    """
    Load required and optional artifacts from a run directory.
    - Required: run_meta.json, instances.json, timeline.json
    - Optional: eval_report.json, review.json, review_template.json, messages.normalized.jsonl
    """
    run_dir = Path(run_dir)
    run_id = run_dir.name

    # Required
    missing_required: List[str] = []
    run_meta_path = run_dir / "run_meta.json"
    instances_path = run_dir / "instances.json"
    timeline_path = run_dir / "timeline.json"
    if not run_meta_path.exists():
        missing_required.append(str(run_meta_path.name))
    if not instances_path.exists():
        missing_required.append(str(instances_path.name))
    if not timeline_path.exists():
        missing_required.append(str(timeline_path.name))
    if missing_required:
        raise FileNotFoundError(
            f"Run incomplete—execute Stage 3 first. Missing: {', '.join(missing_required)}"
        )

    run_meta = _read_json_file(run_meta_path)
    instances = _read_json_file(instances_path)
    timeline = _read_json_file(timeline_path)

    # Optional
    eval_report = None
    er = run_dir / "eval_report.json"
    if er.exists():
        try:
            eval_report = _read_json_file(er)
        except Exception:
            eval_report = {"error": "Failed to parse eval_report.json"}

    review = None
    rv = run_dir / "review.json"
    if rv.exists():
        try:
            review = _read_json_file(rv)
        except Exception:
            review = {"error": "Failed to parse review.json", "rows": []}

    review_template = None
    rvt = run_dir / "review_template.json"
    if rvt.exists():
        try:
            review_template = _read_json_file(rvt)
        except Exception:
            review_template = {"error": "Failed to parse review_template.json", "rows": []}

    normalized_messages_path = None
    nm = run_dir / "messages.normalized.jsonl"
    if nm.exists():
        normalized_messages_path = nm

    return RunData(
        run_id=run_id,
        run_dir=run_dir,
        run_meta=run_meta,
        instances=instances,
        timeline=timeline,
        eval_report=eval_report,
        review=review,
        review_template=review_template,
        normalized_messages_path=normalized_messages_path,
    )


@_cache_data
def index_messages(normalized_jsonl_path: Path) -> Dict[str, Dict[str, Any]]:
    """
    Index normalized messages by message_id. Uses cache to avoid repeated loading.
    """
    normalized_jsonl_path = Path(normalized_jsonl_path)
    index: Dict[str, Dict[str, Any]] = {}
    for obj in read_jsonl(normalized_jsonl_path):
        message_id = obj.get("message_id")
        if not message_id:
            continue
        # Keep a compact subset commonly used in the UI
        index[message_id] = {
            "message_id": message_id,
            "timestamp": obj.get("timestamp"),
            "source": obj.get("source"),
            "sender": obj.get("sender") or obj.get("sender_name"),
            "subject": obj.get("subject"),
            "recipients": obj.get("recipients"),
            "thread_id": obj.get("thread_id"),
            # Keep full obj if needed
            "raw": obj,
        }
    return index


# Catalog helpers (Phase C)
@_cache_data
def load_process_catalog(path: Path):
    try:
        workflow_def = Path("config/workflow_definition.yaml")
        return load_unified_catalog(workflow_def, Path(path))
    except Exception:
        return None


def available_processes(instances: Dict[str, Any] | List[Dict[str, Any]], process_catalog) -> List[str]:
    """
    Derive available canonical processes from instances, intersected with catalog if provided.
    """
    if isinstance(instances, dict):
        items = instances.get("instances") or []
    else:
        items = instances or []
    present = {i.get("canonical_process") for i in items if i.get("canonical_process")}
    if process_catalog:
        catalog_keys = set(process_catalog.processes.keys())
        return sorted(list(present & catalog_keys))
    return sorted(list(present))
