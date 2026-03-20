"""Pipeline Health: run history, durations, error logs, and manual trigger."""
from __future__ import annotations

from datetime import datetime

import httpx
import pandas as pd
import streamlit as st

from dashboard.components.charts import duration_histogram

st.title("Pipeline Health")

API_BASE = __import__("os").environ.get("STREAMLIT_API_BASE_URL", "http://localhost:8000")


@st.cache_data(ttl=30)
def fetch_runs(limit: int = 50) -> list[dict]:
    try:
        resp = httpx.get(f"{API_BASE}/pipeline/runs", params={"limit": limit}, timeout=10)
        resp.raise_for_status()
        return resp.json().get("runs", [])
    except Exception as exc:
        st.warning(f"Could not fetch pipeline runs: {exc}")
        return []


runs = fetch_runs()

# ── Summary metrics ───────────────────────────────────────────────────────
if runs:
    df = pd.DataFrame(runs)
    df["start_time"] = pd.to_datetime(df["start_time"])
    df["end_time"] = pd.to_datetime(df["end_time"])
    df["duration_minutes"] = (df["end_time"] - df["start_time"]).dt.total_seconds() / 60

    success_rate = (df["status"] == "SUCCESS").mean() * 100
    avg_duration = df["duration_minutes"].dropna().mean()

    col1, col2, col3 = st.columns(3)
    col1.metric("Success Rate (last 50)", f"{success_rate:.1f}%")
    col2.metric("Avg Duration", f"{avg_duration:.1f} min" if not pd.isna(avg_duration) else "N/A")
    col3.metric("Total Runs", len(df))

    st.divider()

    # Duration histogram
    valid_durations = df[df["duration_minutes"].notna()]
    if not valid_durations.empty:
        st.plotly_chart(duration_histogram(valid_durations), use_container_width=True)

    st.divider()

    # Run history table
    st.subheader("Run History")
    display_cols = ["run_id", "dag_id", "start_time", "status", "files_processed",
                    "rows_inserted", "rows_updated"]
    display_df = df[display_cols].copy()

    def color_status(val):
        if val == "SUCCESS":
            return "background-color: #d4edda"
        elif val == "FAILED":
            return "background-color: #f8d7da"
        return ""

    styled = display_df.style.applymap(color_status, subset=["status"])
    st.dataframe(styled, use_container_width=True, hide_index=True)

    # Failed runs detail
    failed = df[df["status"] == "FAILED"]
    if not failed.empty:
        st.divider()
        st.subheader("Failed Runs — Error Detail")
        for _, row in failed.iterrows():
            with st.expander(f"{row['run_id']} — {row.get('start_time', '')}"):
                st.code(row.get("error_message", "No error message recorded"))

else:
    st.info("No pipeline runs found yet.")

# ── Manual trigger ─────────────────────────────────────────────────────────
st.divider()
st.subheader("Trigger Pipeline Manually")
with st.form("trigger_form"):
    dag_id = st.selectbox(
        "DAG",
        ["dag_full_pipeline", "dag_extract_to_minio", "dag_transform_to_oracle"],
    )
    run_date = st.date_input("Run Date", value=datetime.today())
    submitted = st.form_submit_button("Trigger")

    if submitted:
        try:
            resp = httpx.post(
                f"{API_BASE}/pipeline/trigger",
                json={"dag_id": dag_id, "run_date": str(run_date)},
                timeout=15,
            )
            resp.raise_for_status()
            result = resp.json()
            st.success(f"Pipeline triggered! Run ID: `{result.get('run_id')}`")
        except Exception as exc:
            st.error(f"Failed to trigger: {exc}")
