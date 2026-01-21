from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path

import json
import pytest

from demo.ingestion.config import IngestionConfig, compute_window
from demo.ingestion.models import build_slack_thread_id, build_gmail_query, RawMessage
from demo.ingestion.normalize import dedup_and_sort


def _cfg_rel(weeks: int = 12) -> IngestionConfig:
    return IngestionConfig(
        raw={
            "dataset": {"dataset_id": "demo", "window": {"mode": "relative", "weeks": weeks}},
            "credentials": {"file": "secrets/credentials.json"},
        }
    )


def _cfg_abs(start: str, end: str) -> IngestionConfig:
    return IngestionConfig(
        raw={
            "dataset": {"dataset_id": "demo", "window": {"mode": "absolute", "start_date": start, "end_date": end}},
            "credentials": {"file": "secrets/credentials.json"},
        }
    )


def test_compute_window_relative():
    cfg = _cfg_rel(weeks=12)
    start_dt, end_dt, start_date, end_date = compute_window(cfg)
    assert (end_dt - start_dt) >= timedelta(weeks=12) - timedelta(days=1)
    assert len(start_date) == 10 and len(end_date) == 10


def test_compute_window_absolute():
    cfg = _cfg_abs("2025-01-01", "2025-02-01")
    start_dt, end_dt, start_date, end_date = compute_window(cfg)
    assert start_date == "2025-01-01"
    assert end_date == "2025-02-01"
    assert end_dt > start_dt


def test_slack_thread_id_generation():
    # with thread_ts
    assert build_slack_thread_id("C123", "1700000000.000", "1699999999.000") == "C123:1700000000.000"
    # without thread_ts
    assert build_slack_thread_id("C123", None, "1699999999.000") == "C123:1699999999.000"


def test_gmail_query_builder():
    q = build_gmail_query("2025-01-01", "2025-02-01", extra="subject:Interview")
    assert "after:2025/01/01" in q and "before:2025/02/01" in q and "subject:Interview" in q


def test_dedup_and_sort():
    from demo.ingestion.models import IngestionInfo, SlackMeta
    # minimal valid RawMessage instances
    ing = IngestionInfo(
        dataset_id="demo",
        time_window={"start": "2025-01-01", "end": "2025-02-01"},
        rules_matched=[],
        source_ref={},
        ingested_at="2025-02-01T00:00:00",
    )
    r1 = RawMessage(
        id="slack_C1_1699999999.000",
        source="slack",
        ts="2025-01-05T00:00:00",
        thread_id="C1:1699999999.000",
        text="a",
        ingestion=ing,
    )
    r2 = RawMessage(
        id="slack_C1_1699999999.000",  # duplicate
        source="slack",
        ts="2025-01-06T00:00:00",
        thread_id="C1:1699999999.000",
        text="b",
        ingestion=ing,
    )
    r3 = RawMessage(
        id="gmail_123",
        source="gmail",
        ts="2025-01-04T00:00:00",
        thread_id="t1",
        text="c",
        ingestion=ing,
    )
    sorted_unique = dedup_and_sort([r1, r2, r3])
    assert [rm.id for rm in sorted_unique] == ["gmail_123", "slack_C1_1699999999.000"]


def test_raw_message_schema_minimal():
    from demo.ingestion.models import IngestionInfo
    ing = IngestionInfo(
        dataset_id="demo",
        time_window={"start": "2025-01-01", "end": "2025-02-01"},
        rules_matched=[],
        source_ref={},
        ingested_at="2025-02-01T00:00:00",
    )
    rm = RawMessage(
        id="gmail_1",
        source="gmail",
        ts="2025-01-03T12:00:00",
        thread_id=None,
        text="hello",
        ingestion=ing,
    )
    assert rm.source == "gmail"
