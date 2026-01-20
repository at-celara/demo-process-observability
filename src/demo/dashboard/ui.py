from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import streamlit as st


def _safe_get(d: Dict[str, Any], path: List[str], default: Any = None) -> Any:
    cur: Any = d
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def render_metrics(run_data: Dict[str, Any]) -> None:
    """
    Render top-level run metrics: #instances, mean confidence, counts by status.
    """
    stats = _safe_get(run_data, ["stats"], {}) or {}
    instances_count = _safe_get(run_data, ["counts", "instances"]) or len(
        (run_data.get("instances") or {}).get("instances", [])
    )
    mean_conf = stats.get("mean_instance_confidence", 0.0)
    status_counts = stats.get("instances_by_status", {})

    cols = st.columns(3)
    cols[0].metric("Instances", f"{instances_count}")
    cols[1].metric("Mean confidence", f"{mean_conf:.2f}")
    cols[2].metric(
        "Statuses",
        " | ".join(f"{k}:{v}" for k, v in status_counts.items()) or "n/a",
    )


def _instances_to_dataframe(
    instances: List[Dict[str, Any]],
    review_map: Dict[str, Dict[str, Any]],
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for inst in instances:
        state = inst.get("state") or {}
        rows.append(
            {
                "instance_key": inst.get("instance_key"),
                "candidate_client": inst.get("candidate_client"),
                "candidate_process": inst.get("candidate_process"),
                "candidate_role": inst.get("candidate_role"),
                "status": state.get("status"),
                "step": state.get("step"),
                "confidence": state.get("confidence"),
                "last_updated_at": state.get("last_updated_at"),
                "review_label": (review_map.get(inst.get("instance_key") or "") or {}).get("human_label"),
            }
        )
    df = pd.DataFrame(rows)
    # Provide stable ordering
    if "confidence" in df.columns:
        df = df.sort_values(by=["confidence"], ascending=False)
    return df


def render_instances_table(
    instances: List[Dict[str, Any]],
    review_map: Dict[str, Dict[str, Any]],
    filters: Dict[str, Any],
) -> Optional[str]:
    """
    Render a filterable instances table and return the selected instance_key if any.
    """
    st.subheader("Instances")
    df = _instances_to_dataframe(instances, review_map)

    # Filters UI
    with st.expander("Filters", expanded=True):
        statuses = sorted([s for s in df["status"].dropna().astype(str).unique().tolist()]) if not df.empty else []
        selected_status = st.multiselect("Status", options=statuses, default=filters.get("status") or [])
        min_conf = float(st.slider("Min confidence", 0.0, 1.0, float(filters.get("min_confidence") or 0.0), 0.01))
        search = st.text_input("Search (client/process/role/summary)", value=filters.get("search") or "")

    # Apply filters
    if selected_status:
        df = df[df["status"].isin(selected_status)]
    df = df[df["confidence"].fillna(0.0) >= min_conf]
    if search:
        needle = search.lower()
        def _contains(s: Any) -> bool:
            return isinstance(s, str) and needle in s.lower()
        df = df[
            df["candidate_client"].apply(_contains)
            | df["candidate_process"].apply(_contains)
            | df["candidate_role"].apply(_contains)
        ]

    # Selection control
    sel = st.selectbox(
        "Select an instance",
        options=[""] + df["instance_key"].tolist(),
        index=0,
        format_func=lambda x: x if x else "—",
    )

    st.dataframe(
        df,
        hide_index=True,
        use_container_width=True,
    )
    return sel or None


def render_state_card(instance: Dict[str, Any]) -> None:
    state = instance.get("state") or {}
    st.markdown(f"### {instance.get('candidate_client') or '—'} • {instance.get('candidate_process') or '—'} • {instance.get('candidate_role') or '—'}")
    cols = st.columns(4)
    cols[0].metric("Status", str(state.get("status") or "unknown"))
    cols[1].metric("Step", str(state.get("step") or "—"))
    cols[2].metric("Confidence", f"{float(state.get('confidence') or 0.0):.2f}")
    cols[3].metric("Last updated", str(state.get("last_updated_at") or "—"))
    summary = state.get("summary")
    if summary:
        st.write(summary)


def render_evidence_timeline(
    evidence_items: List[Dict[str, Any]],
    message_index: Optional[Dict[str, Dict[str, Any]]] = None,
) -> None:
    st.subheader("Evidence timeline")
    if not evidence_items:
        st.info("No evidence available for this instance.")
        return
    for item in evidence_items:
        mid = item.get("message_id")
        timestamp = item.get("timestamp") or "—"
        header = f"{timestamp} • {item.get('event_type') or 'event'} • conf={item.get('confidence')}"
        with st.expander(header):
            st.write(item.get("snippet") or "")
            if message_index and mid and mid in message_index:
                msg = message_index[mid]
                st.caption(
                    f"source={msg.get('source')}  sender={msg.get('sender') or '—'}  "
                    f"subject={msg.get('subject') or '—'}  thread_id={msg.get('thread_id') or '—'}"
                )
                if msg.get("recipients"):
                    st.caption(f"recipients={', '.join(msg['recipients'])}")
