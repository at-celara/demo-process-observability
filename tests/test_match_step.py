from __future__ import annotations

from pathlib import Path

from demo.catalog.loader import load_unified_catalog
from demo.catalog.canonicalize import match_step


def test_match_step_exact_alias_none_and_null():
    pc = load_unified_catalog(
        Path("config/workflow_definition.yaml"),
        Path("tests/fixtures/process_catalog.valid.yml"),
    )

    exact = match_step("role-details", "recruiting", pc, return_details=True)
    assert exact["step_id"] == "role-details"
    assert exact["match_type"] == "exact"
    assert exact["score"] == 1.0

    alias = match_step("Role Details", "recruiting", pc, return_details=True)
    assert alias["step_id"] == "role-details"
    assert alias["match_type"] == "alias"
    assert alias["score"] == 1.0

    fuzzy = match_step("completed role details yesterday", "recruiting", pc, return_details=True)
    assert fuzzy["step_id"] == "role-details"
    assert fuzzy["match_type"] == "fuzzy"
    assert 0.0 < float(fuzzy["score"]) <= 1.0

    none_match = match_step("unknown", "recruiting", pc, return_details=True)
    assert none_match["step_id"] is None
    assert none_match["match_type"] == "none"
    assert none_match["score"] == 0.0

    null_match = match_step(None, "recruiting", pc, return_details=True)
    assert null_match["step_id"] is None
    assert null_match["match_type"] == "none"
    assert null_match["score"] == 0.0
