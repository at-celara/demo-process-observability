from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

from ..ui import format_instance_name, format_progress


def _to_df(instances: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for inst in instances:
        state = inst.get("state") or {}
        rows.append(
            {
                "instance_key": inst.get("instance_key"),
                "Name": format_instance_name(inst),
                "Health": inst.get("health") or "unknown",
                "Owner": inst.get("owner"),
                "Process": inst.get("canonical_process") or inst.get("candidate_process") or "Unknown",
                "Status": state.get("status"),
                "Current step": state.get("step"),
                "Progress": format_progress(inst),
                "Last updated": state.get("last_updated_at"),
                "Confidence": state.get("confidence"),
                "view": False,
            }
        )
    return pd.DataFrame(rows)


def render(instances: List[Dict[str, Any]]) -> Optional[str]:
    st.subheader("Portfolio")
    df = _to_df(instances)

    with st.expander("Filters", expanded=True):
        processes = sorted([p for p in df["Process"].dropna().astype(str).unique().tolist() if p])
        selected_proc = st.multiselect("Process", options=processes, default=[])
        healths = sorted([h for h in df["Health"].dropna().astype(str).unique().tolist() if h])
        selected_health = st.multiselect("Health", options=healths, default=[])
        owners = sorted([o for o in df["Owner"].dropna().astype(str).unique().tolist() if o])
        selected_owner = st.multiselect("Owner", options=owners, default=[])
        clients = sorted({  # derive clients from Name formatting or add explicit column if needed
            (inst.get("canonical_client") or inst.get("candidate_client") or "")
            for inst in instances
        } - {""})
        selected_clients = st.multiselect("Client", options=clients, default=[])
        min_conf = float(st.slider("Min confidence", 0.0, 1.0, 0.0, 0.01))
        search = st.text_input("Search")

    # Apply filters
    if selected_proc:
        df = df[df["Process"].isin(selected_proc)]
    if selected_health:
        df = df[df["Health"].isin(selected_health)]
    if selected_owner:
        df = df[df["Owner"].isin(selected_owner)]
    if selected_clients:
        mask = df["Name"].apply(lambda s: any(c in s for c in selected_clients))
        df = df[mask]
    df = df[df["Confidence"].fillna(0.0) >= min_conf]
    if search:
        needle = search.lower()
        df = df[df["Name"].str.lower().str.contains(needle) | df["Process"].str.lower().str.contains(needle)]

    edited = st.data_editor(
        df,
        hide_index=True,
        use_container_width=True,
        column_config={"view": st.column_config.CheckboxColumn("View", help="Open in Instance Detail")},
        disabled=[c for c in df.columns if c != "view"],
        key="portfolio_editor",
    )
    # Detect any toggled row
    selected = edited[edited["view"] == True]  # noqa: E712
    if not selected.empty:
        # Only take the first toggled
        row = selected.iloc[0]
        return str(row["instance_key"])
    return None

