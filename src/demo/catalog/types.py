from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class HealthSpec:
    at_risk_after_days: int
    overdue_after_days: int


@dataclass
class CatalogProcess:
    process_id: str
    display_name: str
    owner: Optional[str]
    steps: List[str]
    process_aliases: List[str] = field(default_factory=list)
    step_aliases: Dict[str, List[str]] = field(default_factory=dict)
    health: HealthSpec = field(default_factory=lambda: HealthSpec(at_risk_after_days=7, overdue_after_days=14))
    phases: Optional[List[str]] = None
    step_to_phase: Optional[Dict[str, str]] = None


@dataclass
class ProcessCatalog:
    processes: Dict[str, CatalogProcess]
