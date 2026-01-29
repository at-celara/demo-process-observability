from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from demo.catalog.loader import load_unified_catalog
from demo.catalog.loaders import load_clients_catalog, load_roles_catalog
from demo.pipeline.stage3_postprocess import enrich_instances


def _now():
    return datetime(2026, 1, 20, 12, 0, 0, tzinfo=timezone.utc)


def test_phase_b_steps_state_match_and_unknown():
    pc = load_unified_catalog(
        Path("config/workflow_definition.yaml"),
        Path("tests/fixtures/process_catalog.valid.yml"),
    )
    cc = load_clients_catalog(Path("tests/fixtures/clients.valid.yml"))
    rc = load_roles_catalog(Path("tests/fixtures/roles.valid.yml"))

    inst = [
        {
            "instance_key": "thread:1",
            "candidate_client": "Altum",
            "candidate_process": "Recruiting",
            "candidate_role": "AI Engineer",
            "state": {
                "status": "in_progress",
                "step": "Interview Scheduling",
                "confidence": 0.7,
                "last_updated_at": "2026-01-19T00:00:00Z",
            },
            "evidence": [],
        },
        {
            "instance_key": "thread:2",
            "candidate_client": "Altum",
            "candidate_process": "Recruiting",
            "candidate_role": "AI Engineer",
            "state": {"status": "in_progress", "step": "mystery", "confidence": 0.7, "last_updated_at": "2026-01-19T00:00:00Z"},
            "evidence": [],
        },
    ]
    enriched, _ = enrich_instances(inst, pc, cc, rc, _now())
    e1 = enriched[0]
    assert e1["steps_total"] == len(pc.processes["recruiting"].steps)
    assert e1["steps_state"]["role-details"] == "completed"
    assert e1["steps_state"]["interview-scheduling"] == "in_progress"
    e2 = enriched[1]
    assert e2["steps_state"]["role-details"] == "unknown"
