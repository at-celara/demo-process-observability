from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


def load_raw_dataset(path: str | Path) -> Dict[str, Any] | List[Dict[str, Any]]:
    """
    Load a JSON or JSONL dataset from disk.
    Returns either:
      - a dict containing keys 'meta' and/or 'messages'
      - or a list of message dicts (fallback)
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Raw dataset not found at: {p}")
    # Heuristic: if file endswith .jsonl or does not begin with '{' or '[' → treat as JSONL
    suffix = p.suffix.lower()
    with p.open("r", encoding="utf-8") as f:
        head = f.read(1)
        f.seek(0)
        if suffix == ".jsonl" or head not in ("{", "["):
            # JSON Lines → return list of objects
            items: List[Dict[str, Any]] = []
            for line in f:
                line = line.strip()
                if not line:
                    continue
                items.append(json.loads(line))
            return items
        else:
            return json.load(f)


def extract_messages(dataset: Dict[str, Any] | List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extract messages from dataset.
    Rules:
      - If top-level has 'messages' and it's a list → use it
      - Else if top-level is itself a list → treat it as messages (fallback)
      - Otherwise raise ValueError
    """
    if isinstance(dataset, dict):
        messages = dataset.get("messages")
        if isinstance(messages, list):
            return messages
        raise ValueError("Dataset is an object but missing 'messages' list at top level.")
    elif isinstance(dataset, list):
        return dataset
    else:
        raise ValueError("Dataset should be either an object or an array at the top level.")
