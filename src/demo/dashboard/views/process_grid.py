from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

from ..ui import format_instance_name, step_state_to_symbol


def _build_grid(
    instances: List[Dict[str, Any]],
    process_key: str,
    process_catalog,
) -> pd.DataFrame:
    steps = list((process_catalog.processes.get(process_key).steps) if process_catalog and process_key in process_catalog.processes else [])
    rows: List[Dict[str, Any]] = []
    for inst in instances:
        if inst.get("canonical_process") != process_key:
            continue
        state = inst.get("state") or {}
        row = {
            "instance_key": inst.get("instance_key"),
            "Name": format_instance_name(inst),
            "Health": inst.get("health") or "unknown",
            "Owner": inst.get("owner"),
        }
        stmap = inst.get("steps_state") or {}
        for s in steps:
            row[s] = step_state_to_symbol(stmap.get(s))
        row["Last updated"] = state.get("last_updated_at")
        row["Confidence"] = state.get("confidence")
        row["view"] = False
        rows.append(row)
    return pd.DataFrame(rows), steps


def render(instances: List[Dict[str, Any]], process_catalog, available_proc: List[str]) -> Optional[str]:
    st.subheader("Process Grid")
    if not available_proc:
        st.info("No cataloged processes available in this run.")
        return None
    selected_proc = st.selectbox("Process", options=available_proc, index=0, key="process_grid_selected_proc")
    df, steps = _build_grid(instances, selected_proc, process_catalog)

    with st.expander("Filters", expanded=True):
        healths = sorted([h for h in df["Health"].dropna().astype(str).unique().tolist() if h])
        selected_health = st.multiselect("Health", options=healths, default=[])
        min_conf = float(st.slider("Min confidence", 0.0, 1.0, 0.0, 0.01, key="pg_min_conf"))
        search = st.text_input("Search", key="pg_search")

    if selected_health:
        df = df[df["Health"].isin(selected_health)]
    df = df[df["Confidence"].fillna(0.0) >= min_conf]
    if search:
        needle = search.lower()
        df = df[df["Name"].str.lower().str.contains(needle)]

    edited = st.data_editor(
        df,
        hide_index=True,
        use_container_width=True,
        column_config={"view": st.column_config.CheckboxColumn("View", help="Open in Instance Detail")},
        disabled=[c for c in df.columns if c != "view"],
        key="process_grid_editor",
    )
    selected = edited[edited["view"] == True]  # noqa: E712
    if not selected.empty:
        row = selected.iloc[0]
        return str(row["instance_key"])
    return None

