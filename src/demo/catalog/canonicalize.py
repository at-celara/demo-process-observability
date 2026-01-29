from __future__ import annotations

import re
import string
from typing import Dict, List, Optional, Tuple

from .models import ClientsCatalog, RolesCatalog
from .normalize import normalize_text
from .types import ProcessCatalog


_SPACE_RE = re.compile(r"\s+")
_PUNCT_TABLE = str.maketrans({c: " " for c in ",.;:()[]{}<>/\\|"})


def norm_text(s: str) -> str:
    if s is None:
        return ""
    s = " ".join(str(s).strip().split())
    return s.lower()


def norm_tokenish(s: str) -> str:
    if s is None:
        return ""
    s = str(s).translate(_PUNCT_TABLE)
    s = _SPACE_RE.sub(" ", s)
    return s.strip().lower()


def _unique(values: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def canonicalize_process(process_raw: str | None, process_catalog: ProcessCatalog) -> str | None:
    if not process_raw:
        return None
    raw_norm = normalize_text(process_raw)
    if not raw_norm:
        return None
    # 1) exact match on process key
    for k in process_catalog.processes.keys():
        if raw_norm == normalize_text(k):
            return k
    # 2) exact match on display_name
    for k, spec in process_catalog.processes.items():
        if raw_norm == normalize_text(spec.display_name):
            return k
    # 3) exact match on any process_aliases
    for k, spec in process_catalog.processes.items():
        for alias in (spec.process_aliases or []):
            if raw_norm == normalize_text(alias):
                return k
    # 4) substring match on aliases (unambiguous)
    candidates: List[str] = []
    for k, spec in process_catalog.processes.items():
        for alias in (spec.process_aliases or []):
            an = normalize_text(alias)
            if an and (an in raw_norm or raw_norm in an):
                candidates.append(k)
    candidates = _unique(candidates)
    if len(candidates) == 1:
        return candidates[0]
    return None


def canonicalize_client(client_raw: str | None, clients_catalog: ClientsCatalog) -> str | None:
    if client_raw is None:
        return None
    raw_norm = norm_text(client_raw)
    raw_tok = norm_tokenish(client_raw)
    # 1) exact match on name
    for c in clients_catalog.clients:
        if raw_norm == norm_text(c.name):
            return c.name
    # 2) exact match on aliases
    for c in clients_catalog.clients:
        for alias in (c.aliases or []):
            if raw_norm == norm_text(alias):
                return c.name
    # 3) weak substring match (space-collapsed) if unique
    candidates: List[str] = []
    for c in clients_catalog.clients:
        for token in [c.name] + (c.aliases or []):
            tn = norm_text(token)
            if tn in raw_norm or raw_norm in tn:
                candidates.append(c.name)
                break
    candidates = _unique(candidates)
    if len(candidates) == 1:
        return candidates[0]
    # 4) tokenized containment (remove punctuation), e.g., map 'john@acme-inc.com' -> 'Acme Inc'
    tok_hits: List[str] = []
    for c in clients_catalog.clients:
        tokens = [norm_tokenish(c.name)] + [norm_tokenish(a) for a in (c.aliases or [])]
        tokens = [t for t in tokens if t]
        for t in tokens:
            if t and (t in raw_tok or raw_tok in t):
                tok_hits.append(c.name)
                break
    tok_hits = _unique(tok_hits)
    if len(tok_hits) == 1:
        return tok_hits[0]
    # No match: return cleaned original (Title Case)
    return " ".join(w.capitalize() for w in raw_norm.split())


def canonicalize_role(role_raw: str | None, roles_catalog: RolesCatalog) -> str:
    if role_raw is None or not str(role_raw).strip():
        return "Unknown"
    raw_norm = norm_text(role_raw)
    # exact canonical
    for r in roles_catalog.canonical:
        if raw_norm == norm_text(r):
            return r
    # alias to canonical
    for canon, aliases in (roles_catalog.aliases or {}).items():
        for alias in aliases:
            if raw_norm == norm_text(alias):
                return canon
    # else Other
    return "Other"


def match_step(
    step_raw: str | None,
    canonical_process: str | None,
    process_catalog: ProcessCatalog,
    return_details: bool = False,
) -> str | None | Dict[str, float | str | None]:
    """
    Match a raw step string to a canonical step.

    When return_details=False (default), returns step_id or None (backward-compatible).
    When return_details=True, returns:
      { "step_id": str|None, "match_type": "exact|alias|fuzzy|none", "score": float }
    """
    if not step_raw or not canonical_process:
        result = {"step_id": None, "match_type": "none", "score": 0.0}
        return result if return_details else None
    if canonical_process not in process_catalog.processes:
        result = {"step_id": None, "match_type": "none", "score": 0.0}
        return result if return_details else None
    spec = process_catalog.processes[canonical_process]
    step_n = normalize_text(step_raw)
    if not step_n:
        result = {"step_id": None, "match_type": "none", "score": 0.0, "matched_alias": None}
        return result if return_details else None
    # 1) exact match on steps
    for s in spec.steps:
        if norm_text(step_raw) == norm_text(s):
            result = {"step_id": s, "match_type": "exact", "score": 1.0, "matched_alias": None}
            return result if return_details else s
    # 2) exact match on step_aliases
    for canon_step, aliases in (spec.step_aliases or {}).items():
        for alias in aliases:
            if step_n == normalize_text(alias):
                result = {
                    "step_id": canon_step,
                    "match_type": "alias",
                    "score": 1.0,
                    "matched_alias": alias,
                }
                return result if return_details else canon_step
    # 3) substring unique
    candidates: List[str] = []
    candidate_scores: Dict[str, float] = {}
    candidate_alias: Dict[str, str] = {}
    for s in spec.steps:
        sn = normalize_text(s)
        # Only match when the full step name appears in the raw text (not the other way around)
        if sn in step_n:
            candidates.append(s)
            candidate_scores[s] = len(sn) / max(len(step_n), 1)
            candidate_alias[s] = s
    for canon_step, aliases in (spec.step_aliases or {}).items():
        for alias in aliases:
            an = normalize_text(alias)
            # Only match when the full alias appears in the raw text (not the other way around)
            if an in step_n:
                candidates.append(canon_step)
                candidate_scores[canon_step] = max(
                    candidate_scores.get(canon_step, 0.0),
                    len(an) / max(len(step_n), 1),
                )
                candidate_alias[canon_step] = alias
    candidates = _unique(candidates)
    if len(candidates) == 1:
        step_id = candidates[0]
        score = max(0.01, min(1.0, float(candidate_scores.get(step_id, 0.5))))
        result = {
            "step_id": step_id,
            "match_type": "fuzzy",
            "score": score,
            "matched_alias": candidate_alias.get(step_id),
        }
        return result if return_details else step_id
    result = {"step_id": None, "match_type": "none", "score": 0.0, "matched_alias": None}
    return result if return_details else None
