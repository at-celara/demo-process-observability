from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

from ..utils.io import safe_load_json


def _get_store_path() -> Path:
    env_path = os.getenv("WORKFLOW_STORE_PATH")
    return Path(env_path) if env_path else Path("data/workflow_store.json")


def _flatten_workflows(workflows: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for wf in workflows:
        obs = wf.get("observability") or {}
        rows.append(
            {
                "workflow_id": wf.get("workflow_id"),
                "display_name": wf.get("display_name"),
                "process_id": wf.get("process_id"),
                "phase_id": wf.get("phase_id"),
                "client": wf.get("client"),
                "role": wf.get("role"),
                "health": obs.get("health"),
                "confidence": obs.get("confidence"),
                "last_updated_at": obs.get("last_updated_at"),
                "evidence_count": len(obs.get("evidence_message_ids") or []),
                "has_steps": bool(wf.get("steps")),
                "has_phases": bool(wf.get("phases")),
                "view": False,
            }
        )
    return pd.DataFrame(rows)


def _render_detail(workflow: Dict[str, Any]) -> None:
    obs = workflow.get("observability") or {}
    st.subheader(workflow.get("display_name") or workflow.get("workflow_id") or "Workflow")
    cols = st.columns(4)
    cols[0].metric("Process", workflow.get("process_id") or "unknown")
    cols[1].metric("Phase", workflow.get("phase_id") or "unknown")
    cols[2].metric("Health", obs.get("health") or "unknown")
    cols[3].metric("Confidence", f"{float(obs.get('confidence') or 0.0):.2f}")

    st.write(
        {
            "workflow_id": workflow.get("workflow_id"),
            "client": workflow.get("client"),
            "role": workflow.get("role"),
            "last_updated_at": obs.get("last_updated_at"),
            "evidence_count": len(obs.get("evidence_message_ids") or []),
        }
    )

    if obs.get("evidence_message_ids"):
        st.subheader("Evidence message IDs")
        st.write(obs.get("evidence_message_ids"))

    steps = workflow.get("steps")
    if steps:
        st.subheader("Steps")
        st.dataframe(pd.DataFrame(steps), hide_index=True, use_container_width=True)
    phases = workflow.get("phases")
    if phases:
        st.subheader("Phases")
        st.dataframe(pd.DataFrame(phases), hide_index=True, use_container_width=True)

    with st.expander("Raw workflow JSON"):
        st.json(workflow)


def render() -> None:
    st.title("Workflow Store")

    store_path = _get_store_path()
    store, err = safe_load_json(store_path)
    if err:
        st.warning(f"Workflow store not found. Expected at: {store_path}")
        return
    assert store is not None
    workflows = store.get("workflows") or []
    for wf in workflows:
        if wf.get("process_id") == "hiring":
            wf["process_id"] = "recruiting"
    if not workflows:
        st.warning("Workflow store is empty.")
        return

    df = _flatten_workflows(workflows)

    with st.expander("Filters", expanded=True):
        processes = sorted([p for p in df["process_id"].dropna().astype(str).unique().tolist() if p])
        default_proc = "recruiting" if "recruiting" in processes else None
        selected_process = st.selectbox(
            "Process",
            options=[""] + processes,
            index=(processes.index(default_proc) + 1 if default_proc in processes else 0),
        )
        clients = sorted([c for c in df["client"].dropna().astype(str).unique().tolist() if c])
        roles = sorted([r for r in df["role"].dropna().astype(str).unique().tolist() if r])
        healths = sorted([h for h in df["health"].dropna().astype(str).unique().tolist() if h])
        selected_clients = st.multiselect("Client", options=clients, default=[])
        selected_roles = st.multiselect("Role", options=roles, default=[])
        selected_health = st.multiselect("Health", options=healths, default=[])
        has_steps_only = st.checkbox("Has steps/phases only", value=False)

    if selected_process:
        df = df[df["process_id"] == selected_process]
    if selected_clients:
        df = df[df["client"].isin(selected_clients)]
    if selected_roles:
        df = df[df["role"].isin(selected_roles)]
    if selected_health:
        df = df[df["health"].isin(selected_health)]
    if has_steps_only:
        df = df[(df["has_steps"] == True) | (df["has_phases"] == True)]  # noqa: E712

    edited = st.data_editor(
        df,
        hide_index=True,
        use_container_width=True,
        column_config={"view": st.column_config.CheckboxColumn("View")},
        disabled=[c for c in df.columns if c != "view"],
        key="workflow_store_editor",
    )

    selected = edited[edited["view"] == True]  # noqa: E712
    if not selected.empty:
        workflow_id = selected.iloc[0]["workflow_id"]
        st.session_state["selected_workflow_id"] = workflow_id

    selected_id = st.session_state.get("selected_workflow_id")
    if selected_id:
        workflow = next((wf for wf in workflows if wf.get("workflow_id") == selected_id), None)
        if workflow:
            _render_detail(workflow)
        else:
            st.caption("Selected workflow not found in store.")
