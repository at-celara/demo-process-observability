from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional

import streamlit as st

from demo.dashboard.data import RunData, index_messages, list_runs, load_run, load_process_catalog, available_processes
from demo.dashboard.review_store import load_or_init_review, review_map_by_instance_key, save_review
from demo.dashboard.ui import render_evidence_timeline, render_instances_table, render_metrics, render_state_card
from demo.dashboard.views import portfolio as view_portfolio
from demo.dashboard.views import process_grid as view_process_grid
from demo.dashboard.views import instance_detail as view_instance_detail


def _parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--runs-dir", type=str, default="runs")
    parser.add_argument("--run-id", type=str, default="latest")
    # Allow streamlit to pass unknown flags; we only parse after '--'
    return parser.parse_known_intermixed_args(argv)[0]


def _rerun() -> None:
    try:
        # Streamlit >= 1.27
        st.rerun()
    except Exception:
        # Fallback for older versions
        try:
            st.experimental_rerun()  # type: ignore[attr-defined]
        except Exception:
            pass


def _ensure_session_defaults(runs_dir: Path, initial_run_id: Optional[str]) -> None:
    if "runs_dir" not in st.session_state:
        st.session_state["runs_dir"] = str(runs_dir)
    available_runs = list_runs(runs_dir)
    if "selected_run_id" not in st.session_state:
        if initial_run_id and initial_run_id != "latest":
            st.session_state["selected_run_id"] = initial_run_id
        else:
            st.session_state["selected_run_id"] = available_runs[0] if available_runs else None
    if "selected_instance_key" not in st.session_state:
        st.session_state["selected_instance_key"] = None


def _load_selected_run(runs_dir: Path) -> Optional[RunData]:
    run_id = st.session_state.get("selected_run_id")
    if not run_id:
        return None
    run_dir = runs_dir / run_id
    try:
        # Cache bust based on app-level version to reflect saved edits
        version = int(st.session_state.get("data_version", 0))
        return load_run(run_dir, version)
    except FileNotFoundError as e:
        st.error(str(e))
        return None
    except Exception as e:
        st.error(f"Failed to load run: {e}")
        return None


def _sidebar_run_selector(runs_dir: Path) -> None:
    st.sidebar.header("Run")
    runs = list_runs(runs_dir)
    if not runs:
        st.sidebar.warning("No runs found. Execute Stage 1–3 to create artifacts.")
        st.stop()
    idx = 0
    if st.session_state.get("selected_run_id") in runs:
        idx = runs.index(st.session_state["selected_run_id"])
    run_id = st.sidebar.selectbox("Run ID", options=runs, index=idx)
    if run_id != st.session_state.get("selected_run_id"):
        st.session_state["selected_run_id"] = run_id
        st.session_state["selected_instance_key"] = None


def _sidebar_nav() -> str:
    st.sidebar.header("Pages")
    return st.sidebar.radio(
        "Go to",
        options=["Portfolio", "Process Grid", "Instance Detail", "Review", "Evaluation"],
        index=0,
    )


def _overview_page(run: RunData) -> None:
    st.title("Run Overview")
    # Top metrics
    metrics_input = {
        "counts": run.run_meta.get("counts") or {},
        "stats": run.run_meta.get("stats") or {},
        "instances": run.instances,
    }
    render_metrics(metrics_input)

    # Filters memory
    filters = st.session_state.get("overview_filters") or {}
    selected_key = render_instances_table(run.instances.get("instances") or [], review_map_by_instance_key(run.review), filters)
    st.session_state["overview_filters"] = filters
    if selected_key:
        st.session_state["selected_instance_key"] = selected_key
        st.info(f"Selected instance: {selected_key}. Switch to 'Instance Detail' to view.")


def _instance_detail_page(run: RunData) -> None:
    st.title("Instance Detail")
    instances: List[Dict[str, Any]] = run.instances.get("instances") or []
    all_keys = [""] + [inst.get("instance_key") for inst in instances if inst.get("instance_key")]
    preselect = st.session_state.get("selected_instance_key") or ""
    chosen = st.selectbox("Instance", options=all_keys, index=(all_keys.index(preselect) if preselect in all_keys else 0))
    if not chosen:
        st.info("Select an instance to view details.")
        return
    st.session_state["selected_instance_key"] = chosen
    instance = next((i for i in instances if i.get("instance_key") == chosen), None)
    if not instance:
        st.error("Instance not found.")
        return
    render_state_card(instance)
    # Message index if available
    message_index = None
    if run.normalized_messages_path:
        message_index = index_messages(run.normalized_messages_path)
    # Evidence overall timeline (from timeline.json -> by_instance)
    by_instance = run.timeline.get("by_instance") or {}
    evidence_list = by_instance.get(chosen) or []
    render_evidence_timeline(evidence_list, message_index=message_index)
    st.link_button("Jump to Review", "#review")


def _review_page(run: RunData) -> None:
    st.title("Review")
    st.caption("Label model predictions for each instance and save to review.json")
    has_review_file = (run.run_dir / "review.json").exists()
    if not has_review_file:
        st.info("No review.json yet; you can copy from template or start fresh.")
    # Load or init review
    review = load_or_init_review(run.run_dir, run.review_template)
    rows = review.get("rows") or []
    allowed_labels = ["correct", "partial", "incorrect", "unsure"]
    # Quick mapping to find instances
    instances_by_key = {inst.get("instance_key"): inst for inst in (run.instances.get("instances") or [])}

    edited_rows: List[Dict[str, Any]] = []
    for row in rows:
        ikey = row.get("instance_key")
        inst = instances_by_key.get(ikey)
        with st.expander(f"{ikey}"):
            if inst:
                render_state_card(inst)
                # Show top evidence snippets
                evidence = inst.get("evidence") or []
                render_evidence_timeline(evidence)
            label = st.selectbox(
                "Label",
                options=[""] + allowed_labels,
                index=(allowed_labels.index(row.get("human_label")) + 1 if row.get("human_label") in allowed_labels else 0),
                key=f"label-{ikey}",
            )
            notes = st.text_area("Notes", value=row.get("human_notes") or "", key=f"notes-{ikey}")
            corrected_status = st.selectbox(
                "Corrected status (optional)",
                options=[""] + ["in_progress", "blocked", "done", "unknown"],
                index=0,
                key=f"cstatus-{ikey}",
            )
            corrected_step = st.text_input("Corrected step (optional)", value="", key=f"cstep-{ikey}")

            updated = dict(row)
            updated["human_label"] = label or None
            updated["human_notes"] = notes or None
            if corrected_status:
                updated["corrected_status"] = corrected_status
            if corrected_step:
                updated["corrected_step"] = corrected_step
            edited_rows.append(updated)

    # Actions
    cols = st.columns(3)
    if cols[0].button("Copy template → review.json", disabled=has_review_file or not run.review_template):
        if run.review_template:
            save_review(run.run_dir, run.review_template)
            st.success("Copied template to review.json")
            st.experimental_rerun()
    if cols[1].button("Save all changes"):
        save_review(run.run_dir, {"rows": edited_rows})
        st.success("Saved review.json")
        # Bump version to invalidate cached run data
        st.session_state["data_version"] = int(st.session_state.get("data_version", 0)) + 1
        _rerun()

    # Summary
    labels = [r.get("human_label") for r in edited_rows if r.get("human_label")]
    label_counts = Counter(labels)
    if label_counts:
        st.write({k: int(v) for k, v in label_counts.items()})


def _evaluation_page(run: RunData) -> None:
    st.title("Evaluation")
    if run.eval_report and not run.eval_report.get("error"):
        st.subheader("Eval report (from file)")
        st.json(run.eval_report)
    else:
        st.info("No eval_report.json found; showing a lightweight live eval.")
        instances: List[Dict[str, Any]] = run.instances.get("instances") or []
        statuses = Counter([(i.get("state") or {}).get("status", "unknown") for i in instances])
        mean_conf = 0.0
        if instances:
            mean_conf = sum(float((i.get("state") or {}).get("confidence") or 0.0) for i in instances) / len(instances)
        evid_n = sum(1 for i in instances if len(i.get("evidence") or []) >= 3)
        st.metric("Instances", f"{len(instances)}")
        st.metric("Mean confidence", f"{mean_conf:.2f}")
        st.write({"by_status": dict(statuses)})
        st.write({"pct_with_>=3_evidence": round(100.0 * evid_n / max(1, len(instances)), 2)})

    # Optional recompute local eval from review
    st.subheader("Local recompute (optional)")
    if st.button("Recompute eval report (local)"):
        review_path = run.run_dir / "review.json"
        if not review_path.exists():
            st.warning("No review.json to compute from.")
        else:
            try:
                import json
                review = json.loads(review_path.read_text(encoding="utf-8"))
                labels = [row.get("human_label") for row in (review.get("rows") or []) if row.get("human_label")]
                label_counts = Counter(labels)
                report = {
                    "has_human_labels": True,
                    "labels": dict(label_counts),
                    "acceptance_rate": float(
                        (label_counts.get("correct", 0) + label_counts.get("partial", 0))
                        / max(1, sum(label_counts.values()))
                    ),
                }
                out = run.run_dir / "eval_report.local.json"
                out.write_text(__import__("json").dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
                st.success(f"Wrote {out.name}")
            except Exception as e:
                st.error(f"Failed to recompute eval: {e}")


def main() -> None:
    # Parse args after '--' if present
    argv = []
    if "--" in sys.argv:
        argv = sys.argv[sys.argv.index("--") + 1 :]
    args = _parse_args(argv)
    runs_dir = Path(args.runs_dir)
    _ensure_session_defaults(runs_dir, args.run_id)

    _sidebar_run_selector(runs_dir)
    page = _sidebar_nav()

    run = _load_selected_run(runs_dir)
    if run is None:
        st.stop()

    if page == "Portfolio":
        sel = view_portfolio.render(run.instances.get("instances") or [])
        if sel:
            st.session_state["selected_instance_key"] = sel
            _rerun()
    elif page == "Process Grid":
        pc = load_process_catalog(Path("config/process_catalog.yml"))
        procs = available_processes(run.instances, pc)
        sel = view_process_grid.render(run.instances.get("instances") or [], pc, procs)
        if sel:
            st.session_state["selected_instance_key"] = sel
            _rerun()
    elif page == "Instance Detail":
        _instance_detail_page(run)
    elif page == "Review":
        _review_page(run)
    elif page == "Evaluation":
        _evaluation_page(run)
    else:
        st.write("Unknown page.")


if __name__ == "__main__":
    main()
