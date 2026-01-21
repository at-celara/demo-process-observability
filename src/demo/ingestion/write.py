from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List

from ..utils.json_utils import write_json, write_jsonl
from .models import RawMessage


def write_raw_messages(path: Path, items: Iterable[RawMessage]) -> None:
    write_jsonl(path, (rm.model_dump() for rm in items))


def write_json_file(path: Path, obj: Dict[str, Any]) -> None:
    write_json(path, obj)
