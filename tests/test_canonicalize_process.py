from __future__ import annotations

from pathlib import Path

from demo.catalog.loader import load_unified_catalog
from demo.catalog.canonicalize import canonicalize_process


def test_canonicalize_process_basic():
    pc = load_unified_catalog(
        Path("config/workflow_definition.yaml"),
        Path("tests/fixtures/process_catalog.valid.yml"),
    )
    assert canonicalize_process("hiring", pc) == "recruiting"
    assert canonicalize_process("Hiring", pc) == "recruiting"
    assert canonicalize_process("recruiting pipeline", pc) == "recruiting"
    assert canonicalize_process("Recruiting", pc) == "recruiting"
    # Unknown returns None
    assert canonicalize_process("foo", pc) is None
