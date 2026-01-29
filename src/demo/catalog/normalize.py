from __future__ import annotations

import re
import string
from typing import Iterable, List, Tuple

_PUNCT_TABLE = str.maketrans({c: " " for c in string.punctuation})
_SPACE_RE = re.compile(r"\s+")


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    s = str(value).lower()
    s = s.replace("_", " ").replace("-", " ")
    s = s.translate(_PUNCT_TABLE)
    s = _SPACE_RE.sub(" ", s)
    return s.strip()


def dedupe_aliases(aliases: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for alias in aliases:
        key = normalize_text(alias)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(alias)
    return out


def dedupe_aliases_with_keys(aliases: Iterable[Tuple[str, str]]) -> List[str]:
    """
    Accepts (alias, normalized_key) tuples and returns de-duped alias list.
    """
    seen = set()
    out: List[str] = []
    for alias, key in aliases:
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(alias)
    return out
