from __future__ import annotations

from pathlib import Path

from demo.catalog.loaders import load_process_catalog
from demo.catalog.canonicalize import match_step


def test_match_step_exact_alias_none_and_null():
    pc = load_process_catalog(Path("tests/fixtures/process_catalog.valid.yml"))

    exact = match_step("close", "hiring", pc, return_details=True)
    assert exact["step_id"] == "close"
    assert exact["match_type"] == "exact"
    assert exact["score"] == 1.0

    alias = match_step("phone screen", "hiring", pc, return_details=True)
    assert alias["step_id"] == "screening"
    assert alias["match_type"] == "alias"
    assert alias["score"] == 1.0

    fuzzy = match_step("interview loop", "hiring", pc, return_details=True)
    assert fuzzy["step_id"] == "interviews"
    assert fuzzy["match_type"] == "alias"
    assert fuzzy["score"] == 1.0

    none_match = match_step("unknown", "hiring", pc, return_details=True)
    assert none_match["step_id"] is None
    assert none_match["match_type"] == "none"
    assert none_match["score"] == 0.0

    null_match = match_step(None, "hiring", pc, return_details=True)
    assert null_match["step_id"] is None
    assert null_match["match_type"] == "none"
    assert null_match["score"] == 0.0
