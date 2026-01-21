from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.merge_client_datasets import _merge


def _write_json(path: Path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def test_merge_dedup_and_provenance(tmp_path: Path):
    # Two files with overlap on id "gmail_1"
    f1 = tmp_path / "01_raw_messages_altum.json"
    f2 = tmp_path / "01_raw_messages_public_relay.json"
    msgs1 = {
        "meta": {},
        "messages": [
            {"id": "gmail_1", "source": "gmail", "ts": "2025-01-01T00:00:00", "text": "A"},
            {"id": "gmail_2", "source": "gmail", "ts": "2025-01-02T00:00:00", "text": "B"},
        ],
    }
    msgs2 = {
        "meta": {},
        "messages": [
            {"id": "gmail_1", "source": "gmail", "ts": "2025-01-01T00:00:00", "text": "A"},
            {"id": "slack_C1_1", "source": "slack", "ts": "2025-01-03T00:00:00", "text": "C"},
        ],
    }
    _write_json(f1, msgs1)
    _write_json(f2, msgs2)

    out, client_counts = _merge([f1, f2], dataset_id="prod_repo_2025_client_loop")

    assert "meta" in out and "messages" in out
    assert out["meta"]["counts"]["raw_messages_total"] == 4
    assert out["meta"]["counts"]["unique_messages"] == 3
    assert out["meta"]["counts"]["duplicates_removed"] == 1

    # Find the merged gmail_1
    merged = {m["id"]: m for m in out["messages"]}
    assert "gmail_1" in merged
    ing = merged["gmail_1"]["ingestion"]
    assert sorted(ing["matched_clients"]) == ["Altum", "Public Relay"]
    assert sorted(ing["files_seen_in"]) == ["01_raw_messages_altum.json", "01_raw_messages_public_relay.json"]

    # Top counts should include both clients
    assert client_counts["Altum"] >= 1
    assert client_counts["Public Relay"] >= 1
