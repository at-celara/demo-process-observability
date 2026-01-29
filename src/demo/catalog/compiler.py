from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .normalize import dedupe_aliases_with_keys, normalize_text
from .types import CatalogProcess, HealthSpec


def _add_alias(aliases: List[Tuple[str, str]], alias: Optional[str]) -> None:
    if not alias:
        return
    key = normalize_text(alias)
    if not key:
        return
    aliases.append((alias, key))


def _seed_step_aliases(step: Dict[str, Any]) -> List[str]:
    aliases: List[Tuple[str, str]] = []
    step_id = step.get("id")
    name = step.get("name")
    short_name = step.get("short_name")

    _add_alias(aliases, name)
    _add_alias(aliases, short_name)
    _add_alias(aliases, step_id)
    if step_id:
        _add_alias(aliases, step_id.replace("-", " "))
        _add_alias(aliases, step_id.replace("_", " "))
    if name:
        _add_alias(aliases, name.lower())
    if short_name:
        _add_alias(aliases, short_name.lower())

    return dedupe_aliases_with_keys(aliases)


def compile_recruiting(workflow_def: Dict[str, Any], override: Optional[Dict[str, Any]] = None) -> CatalogProcess:
    processes = workflow_def.get("processes") or []
    process = next((p for p in processes if p.get("id") == "recruiting"), None)
    if not process:
        raise ValueError('workflow_definition is missing process id "recruiting"')

    phases = process.get("phases") or []
    phase_ids = [phase.get("id") for phase in phases if phase.get("id")]
    steps: List[str] = []
    step_to_phase: Dict[str, str] = {}
    step_aliases: Dict[str, List[str]] = {}

    for phase in phases:
        phase_id = phase.get("id")
        for step in (phase.get("steps") or []):
            step_id = step.get("id")
            if not step_id:
                continue
            steps.append(step_id)
            if phase_id:
                step_to_phase[step_id] = phase_id
            step_aliases[step_id] = _seed_step_aliases(step)

    # Seed process aliases
    process_aliases: List[Tuple[str, str]] = []
    for alias in ["recruiting", "recruitment", "hiring", "recruiting pipeline", process.get("name")]:
        _add_alias(process_aliases, alias)

    # Health defaults
    health = HealthSpec(at_risk_after_days=7, overdue_after_days=14)

    # Apply overrides
    if override:
        override_proc = (override.get("processes") or {}).get("recruiting") or {}
        for alias in override_proc.get("process_aliases") or []:
            _add_alias(process_aliases, alias)
        for step_id, aliases in (override_proc.get("step_aliases") or {}).items():
            if step_id not in step_aliases:
                step_aliases[step_id] = []
            existing = [(a, normalize_text(a)) for a in step_aliases[step_id]]
            for alias in aliases or []:
                _add_alias(existing, alias)
            step_aliases[step_id] = dedupe_aliases_with_keys(existing)

    return CatalogProcess(
        process_id="recruiting",
        display_name=process.get("name") or "Recruiting",
        owner=process.get("owner") or process.get("owner_role"),
        steps=steps,
        process_aliases=dedupe_aliases_with_keys(process_aliases),
        step_aliases=step_aliases,
        health=health,
        phases=phase_ids,
        step_to_phase=step_to_phase,
    )


def recruiting_debug_summary(catalog: CatalogProcess) -> Dict[str, Any]:
    return {
        "process_id": catalog.process_id,
        "phases": catalog.phases or [],
        "steps": catalog.steps,
        "step_alias_counts": {k: len(v or []) for k, v in (catalog.step_aliases or {}).items()},
        "health_defaults": {
            "at_risk_after_days": catalog.health.at_risk_after_days,
            "overdue_after_days": catalog.health.overdue_after_days,
        },
    }
