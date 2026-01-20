from __future__ import annotations

import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Iterable


def write_json(path: Path, obj: Dict[str, Any]) -> None:
    """
    Pretty-print JSON to UTF-8 file with best-effort atomic write:
    write to temp file in same directory, then replace.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = path.parent

    with NamedTemporaryFile("w", encoding="utf-8", dir=tmp_dir, delete=False) as tmp:
        json.dump(obj, tmp, ensure_ascii=False, indent=2)
        tmp.write("\n")
        tmp_path = Path(tmp.name)

    os.replace(tmp_path, path)


def write_jsonl(path: Path, items: Iterable[Dict[str, Any]]) -> None:
    """
    Write an iterable of dicts to a JSON Lines file with best-effort atomic write.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = path.parent

    with NamedTemporaryFile("w", encoding="utf-8", dir=tmp_dir, delete=False) as tmp:
        for item in items:
            json.dump(item, tmp, ensure_ascii=False)
            tmp.write("\n")
        tmp_path = Path(tmp.name)

    os.replace(tmp_path, path)
