"""Overview page: KPIs, row load trend, dataset status table."""
from __future__ import annotations

from datetime import datetime, timezone

import httpx
import pandas as pd
import streamlit as st

from dashboard.components.charts import rows_over_time, pipeline_status_pie
from dashboard.components.sidebar import date_range_filter

st.title("Overview")

API_BASE = __import__("os").environ.get("STREAMLIT_API_BASE_URL", "http://localhost:8000")


@st.cache_data(ttl=60)
def fetch_datasets() -> list[dict]:
    try:
        resp = httpx.get(f"{API_BASE}/datasets", timeout=10)
        resp.raise_for_status()
        return resp.json().get("datasets", [])
    except Exception as exc:
        st.warning(f"Could not fetch datasets: {exc}")
        return []


@st.cache_data(ttl=60)
def fetch_pipeline_runs(limit: int = 30) -> list[dict]:
    try:
        resp = httpx.get(f"{API_BASE}/pipeline/runs", params={"limit": limit}, timeout=10)
        resp.raise_for_status()
        return resp.json().get("runs", [])
    except Exception as exc:
        st.warning(f"Could not fetch pipeline runs: {exc}")
        return []


# ── KPI Row ────────────────────────────────────────────────────────────────
datasets = fetch_datasets()
runs = fetch_pipeline_runs()

total_rows = sum(d.get("row_count", 0) for d in datasets)
total_datasets = len(datasets)
last_run = runs[0] if runs else {}
last_run_status = last_run.get("status", "N/A")
last_run_rows = last_run.get("rows_inserted", 0) + last_run.get("rows_updated", 0)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Rows in Oracle", f"{total_rows:,}")
col2.metric("Active Datasets", total_datasets)
col3.metric("Last Run Status", last_run_status)
col4.metric("Last Run Rows", f"{last_run_rows:,}")

st.divider()

# ── Rows loaded trend ─────────────────────────────────────────────────────
if runs:
    run_df = pd.DataFrame(runs)
    run_df["date"] = pd.to_datetime(run_df["start_time"]).dt.date.astype(str)
    trend_df = (
        run_df.groupby("date")[["rows_inserted", "rows_updated"]]
        .sum()
        .reset_index()
    )
    trend_df["rows"] = trend_df["rows_inserted"] + trend_df["rows_updated"]
    st.plotly_chart(rows_over_time(trend_df, date_col="date", value_col="rows"), use_container_width=True)

st.divider()

# ── Dataset status table ───────────────────────────────────────────────────
st.subheader("Dataset Status")
if datasets:
    df = pd.DataFrame(datasets)[["name", "row_count", "last_loaded"]]
    df.columns = ["Dataset", "Row Count", "Last Loaded"]
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.info("No datasets found. Run the pipeline to load data.")

# ── Pipeline run status pie ────────────────────────────────────────────────
if runs:
    st.subheader("Run Status (Last 30)")
    status_counts = pd.Series([r.get("status") for r in runs]).value_counts()
    fig = pipeline_status_pie(
        success=int(status_counts.get("SUCCESS", 0)),
        failed=int(status_counts.get("FAILED", 0)),
        running=int(status_counts.get("RUNNING", 0)),
    )
    st.plotly_chart(fig, use_container_width=True)
