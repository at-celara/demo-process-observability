from __future__ import annotations

from pathlib import Path
import pytest

from demo.catalog.loader import load_unified_catalog
from demo.catalog.loaders import load_clients_catalog, load_roles_catalog


def test_load_valid_catalogs(tmp_path: Path):
    base = Path("tests/fixtures")
    pc = load_unified_catalog(
        Path("config/workflow_definition.yaml"),
        base / "process_catalog.valid.yml",
    )
    assert "recruiting" in pc.processes
    assert "hiring" not in pc.processes
    cc = load_clients_catalog(base / "clients.valid.yml")
    assert any(c.name == "Altum" for c in cc.clients)
    rc = load_roles_catalog(base / "roles.valid.yml")
    assert "Other" in rc.canonical and "Unknown" in rc.canonical


def test_invalid_process_catalog_raises(tmp_path: Path):
    wf_path = Path("config/workflow_definition.yaml")
    invalid_path = tmp_path / "process_catalog.invalid.yml"
    invalid_path.write_text(
        """
processes:
  delivery:
    display_name: "Delivery"
    owner: "PM"
    steps: ["kickoff", "Kickoff"]  # duplicate after normalization
    health:
      at_risk_after_days: 10
      overdue_after_days: 5
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError):
        load_unified_catalog(wf_path, invalid_path)
