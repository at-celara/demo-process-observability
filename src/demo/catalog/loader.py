from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from .compiler import compile_recruiting, recruiting_debug_summary
from .normalize import dedupe_aliases, normalize_text
from .types import CatalogProcess, HealthSpec, ProcessCatalog


def _load_yaml(path: Path) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"YAML file not found: {p}")
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
        return data or {}
    except Exception as exc:
        raise ValueError(f"Failed to parse YAML at {p}: {exc}") from exc


def _coerce_health(raw: Dict[str, Any] | None) -> HealthSpec:
    if not raw:
        return HealthSpec(at_risk_after_days=7, overdue_after_days=14)
    return HealthSpec(
        at_risk_after_days=int(raw.get("at_risk_after_days", 7)),
        overdue_after_days=int(raw.get("overdue_after_days", 14)),
    )


def _compile_from_process_catalog(process_id: str, spec: Dict[str, Any]) -> CatalogProcess:
    steps = list(spec.get("steps") or [])
    if not steps:
        raise ValueError(f"process '{process_id}' must define non-empty steps")
    normed = [normalize_text(s) for s in steps]
    if len(set(normed)) != len(normed):
        raise ValueError(f"process '{process_id}' steps must be unique after normalization")
    process_aliases = dedupe_aliases(spec.get("process_aliases") or [])
    step_aliases = {k: dedupe_aliases(v or []) for k, v in (spec.get("step_aliases") or {}).items()}
    health = _coerce_health(spec.get("health"))
    if health.overdue_after_days < health.at_risk_after_days:
        raise ValueError(f"process '{process_id}' health thresholds invalid: overdue < at_risk")
    return CatalogProcess(
        process_id=process_id,
        display_name=spec.get("display_name") or process_id,
        owner=spec.get("owner"),
        steps=steps,
        process_aliases=process_aliases,
        step_aliases=step_aliases,
        health=health,
    )


def load_unified_catalog(
    workflow_definition_path: str | Path,
    process_catalog_path: str | Path,
    override_path: str | Path | None = None,
) -> ProcessCatalog:
    workflow_def = _load_yaml(Path(workflow_definition_path))
    override = None
    if override_path:
        override_candidate = Path(override_path)
        if override_candidate.exists():
            override = _load_yaml(override_candidate)
    recruiting = compile_recruiting(workflow_def, override=override)

    process_catalog_data = _load_yaml(Path(process_catalog_path))
    processes = process_catalog_data.get("processes") or {}
    compiled: Dict[str, CatalogProcess] = {"recruiting": recruiting}
    for process_id, spec in processes.items():
        if process_id in {"hiring", "recruiting"}:
            continue
        compiled[process_id] = _compile_from_process_catalog(process_id, spec or {})

    return ProcessCatalog(processes=compiled)


def compiled_catalog_debug(
    catalog: ProcessCatalog,
    workflow_definition_path: str | Path,
    process_catalog_path: str | Path,
    override_path: str | Path | None = None,
) -> Dict[str, Any]:
    return {
        "process_ids": sorted(list(catalog.processes.keys())),
        "recruiting": recruiting_debug_summary(catalog.processes["recruiting"]),
        "sources": {
            "workflow_definition_path": str(workflow_definition_path),
            "process_catalog_path": str(process_catalog_path),
            "override_path": str(override_path) if override_path else None,
        },
    }
