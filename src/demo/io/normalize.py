from __future__ import annotations

import hashlib
import json
from typing import List, Tuple

from .load_raw import extract_messages
from ..schemas.messages import NormalizedMessage


def _deterministic_id(raw: dict) -> str:
    canonical = json.dumps(raw, sort_keys=True, ensure_ascii=False)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    return f"auto_{digest}"


def normalize_message(raw: dict, keep_raw: bool = True) -> NormalizedMessage:
    message_id = raw.get("id") or _deterministic_id(raw)
    source = raw.get("source", "unknown")

    timestamp = raw.get("ts")
    if not timestamp:
        raise ValueError(f"Missing required timestamp 'ts' in message id={message_id} keys={list(raw.keys())}")

    text = raw.get("text", "")
    thread_id = raw.get("thread_id")

    model = NormalizedMessage(
        message_id=message_id,
        source=source,
        timestamp=timestamp,
        text=text if isinstance(text, str) else str(text),
        thread_id=thread_id,
        sender=raw.get("sender"),
        sender_name=raw.get("sender_name"),
        recipients=raw.get("recipients"),
        subject=raw.get("subject"),
        has_attachments=raw.get("has_attachments"),
        account_email=raw.get("account_email"),
        attachments=raw.get("attachments"),
        raw=raw if keep_raw else None,
    )
    return model


def normalize_messages(
    raw_messages: List[dict],
    sort_by_timestamp: bool,
    keep_raw: bool,
) -> Tuple[List[NormalizedMessage], int]:
    """
    Returns:
      - list of NormalizedMessage
      - empty_text_count
    """
    normalized: List[NormalizedMessage] = []
    empty_text_count = 0
    for raw in raw_messages:
        nm = normalize_message(raw, keep_raw=keep_raw)
        if nm.text == "":
            empty_text_count += 1
        normalized.append(nm)

    if sort_by_timestamp:
        try:
            normalized.sort(key=lambda m: m.timestamp)
        except Exception:
            # Fallback to as-is order if sorting fails; keep it simple as per spec.
            pass

    return normalized, empty_text_count
