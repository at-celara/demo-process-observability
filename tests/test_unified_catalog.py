from __future__ import annotations

import json
from pathlib import Path

from demo.catalog.compiler import compile_recruiting
from demo.catalog.loader import load_unified_catalog
from demo.catalog.normalize import normalize_text
from demo.pipeline.reconciliation import run_reconciliation


def _workflow_def_minimal():
    return {
        "processes": [
            {
                "id": "recruiting",
                "name": "Recruiting",
                "phases": [
                    {
                        "id": "p1",
                        "steps": [
                            {"id": "step-a", "name": "Step A", "short_name": "A"},
                            {"id": "step-b", "name": "Step B"},
                        ],
                    },
                    {
                        "id": "p2",
                        "steps": [
                            {"id": "step-c", "name": "Step C"},
                        ],
                    },
                ],
            }
        ]
    }


def test_compile_recruiting_steps_order():
    catalog = compile_recruiting(_workflow_def_minimal())
    assert catalog.steps == ["step-a", "step-b", "step-c"]
    assert catalog.step_to_phase == {"step-a": "p1", "step-b": "p1", "step-c": "p2"}


def test_compile_recruiting_seed_aliases():
    catalog = compile_recruiting(_workflow_def_minimal())
    aliases = catalog.step_aliases["step-a"]
    normed = {normalize_text(a) for a in aliases}
    assert normalize_text("Step A") in normed
    assert normalize_text("A") in normed
    assert normalize_text("step-a") in normed
    assert normalize_text("step a") in normed


def test_override_merge_dedup():
    override = {
        "processes": {
            "recruiting": {
                "step_aliases": {
                    "step-a": ["Step A", "step a", "extra alias"],
                }
            }
        }
    }
    catalog = compile_recruiting(_workflow_def_minimal(), override=override)
    aliases = catalog.step_aliases["step-a"]
    normed = {normalize_text(a) for a in aliases}
    assert normalize_text("extra alias") in normed
    # deduped by normalized key
    assert len([a for a in aliases if normalize_text(a) == normalize_text("step a")]) == 1


def test_loader_skips_hiring_and_recruiting_in_process_catalog(tmp_path: Path):
    wf_path = tmp_path / "workflow_definition.yml"
    wf_path.write_text(json.dumps(_workflow_def_minimal()), encoding="utf-8")
    pc_path = tmp_path / "process_catalog.yml"
    pc_path.write_text(
        json.dumps(
            {
                "processes": {
                    "hiring": {"display_name": "Hiring", "owner": "HR", "steps": ["s1"], "health": {"at_risk_after_days": 7, "overdue_after_days": 14}},
                    "recruiting": {"display_name": "Recruiting", "owner": "HR", "steps": ["sX"], "health": {"at_risk_after_days": 7, "overdue_after_days": 14}},
                    "delivery": {"display_name": "Delivery", "owner": "PM", "steps": ["kickoff"], "health": {"at_risk_after_days": 7, "overdue_after_days": 14}},
                }
            }
        ),
        encoding="utf-8",
    )
    catalog = load_unified_catalog(wf_path, pc_path)
    assert "recruiting" in catalog.processes
    assert "hiring" not in catalog.processes
    assert "delivery" in catalog.processes
    assert catalog.processes["recruiting"].steps == ["step-a", "step-b", "step-c"]


def test_store_migration_hiring_to_recruiting(tmp_path: Path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    # minimal workflow definition
    wf_path = run_dir / "workflow_definition.yml"
    wf_path.write_text(json.dumps(_workflow_def_minimal()), encoding="utf-8")
    # minimal instances
    instances_path = run_dir / "instances.json"
    instances_path.write_text(
        json.dumps(
            {
                "instances": [
                    {
                        "instance_key": "thread:1",
                        "canonical_process": "recruiting",
                        "canonical_client": "Altum",
                        "canonical_role": "AI Engineer",
                        "candidate_client_raw": "Altum",
                        "candidate_role_raw": "AI Engineer",
                        "state": {"status": "in_progress", "step": "Step A"},
                        "health": "on_track",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "timeline.json").write_text(json.dumps({"by_instance": {}}), encoding="utf-8")

    store_path = tmp_path / "workflow_store.json"
    store_path.write_text(
        json.dumps(
            {
                "version": 1,
                "updated_at": "2026-01-01T00:00:00Z",
                "workflows": [{"workflow_id": "wf_1", "process_id": "hiring"}],
            }
        ),
        encoding="utf-8",
    )

    cfg = {
        "reconciliation": {
            "store": {"persistent_path": str(store_path), "snapshot_name": "workflow_store.snapshot.json"},
            "reports": {
                "coverage_name": "coverage_report.json",
                "reconciliation_name": "reconciliation_report.json",
                "drift_name": "mapping_drift_report.json",
            },
        }
    }
    run_reconciliation(run_dir, cfg)
    data = json.loads(store_path.read_text(encoding="utf-8"))
    assert all(wf.get("process_id") != "hiring" for wf in data.get("workflows") or [])
    assert any(wf.get("process_id") == "recruiting" for wf in data.get("workflows") or [])
