from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import streamlit as st

from ..ui import render_evidence_timeline, render_state_card


def _health_explanation(instance: Dict[str, Any], process_catalog) -> str:
    health = instance.get("health") or "unknown"
    if health == "on_track":
        return "On track."
    state = instance.get("state") or {}
    lu = state.get("last_updated_at")
    status = state.get("status")
    parts: List[str] = []
    if status == "blocked":
        parts.append("blocked")
    if lu:
        try:
            if lu.endswith("Z"):
                dt = datetime.fromisoformat(lu.replace("Z", "+00:00"))
            else:
                dt = datetime.fromisoformat(lu)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            days = int((datetime.now(timezone.utc) - dt).total_seconds() // 86400)
            parts.append(f"stale for {days} days")
        except Exception:
            pass
    return ", ".join(parts) or "unknown"


def render(instance: Dict[str, Any], message_index: Optional[Dict[str, Any]] = None, process_catalog=None) -> None:
    st.subheader("Instance Detail")
    render_state_card(instance)

    # Health explanation
    with st.expander("Health", expanded=True):
        st.write(f"Health: {instance.get('health') or 'unknown'}")
        st.caption(_health_explanation(instance, process_catalog))

    # Steps summary
    steps_state = instance.get("steps_state")
    if steps_state:
        with st.expander("Steps", expanded=False):
            for step, state in steps_state.items():
                st.write(f"- {step}: {state}")

    # Debug fields
    with st.expander("Debug fields"):
        st.json(
            {
                "candidate_process_raw": instance.get("candidate_process_raw"),
                "canonical_process": instance.get("canonical_process"),
                "candidate_client_raw": instance.get("candidate_client_raw"),
                "canonical_client": instance.get("canonical_client"),
                "candidate_role_raw": instance.get("candidate_role_raw"),
                "canonical_role": instance.get("canonical_role"),
                "owner": instance.get("owner"),
            }
        )

    # Evidence
    by_instance = None
    # This view expects caller to pass evidence list directly, so just show what's in the instance? Keep consistency:
    ev = instance.get("evidence") or []
    render_evidence_timeline(ev, message_index=message_index)

