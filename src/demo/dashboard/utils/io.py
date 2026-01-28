from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


def load_json(path: Path) -> Dict[str, Any]:
    path = Path(path)
    return json.loads(path.read_text(encoding="utf-8"))


def safe_load_json(path: Path) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    path = Path(path)
    if not path.exists():
        return None, f"File not found: {path}"
    try:
        return load_json(path), None
    except Exception as exc:
        return None, f"Failed to parse JSON: {path}: {exc}"
