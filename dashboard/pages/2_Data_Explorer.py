"""Data Explorer: Browse Oracle table records with filters and export."""
from __future__ import annotations

import httpx
import pandas as pd
import streamlit as st

from dashboard.components.sidebar import dataset_selector, date_range_filter, row_limit_slider

st.title("Data Explorer")

API_BASE = __import__("os").environ.get("STREAMLIT_API_BASE_URL", "http://localhost:8000")


@st.cache_data(ttl=120)
def fetch_dataset_names() -> list[str]:
    try:
        resp = httpx.get(f"{API_BASE}/datasets", timeout=10)
        resp.raise_for_status()
        return [d["name"] for d in resp.json().get("datasets", [])]
    except Exception:
        return []


@st.cache_data(ttl=30)
def fetch_records(dataset: str, limit: int, offset: int, date_from: str, date_to: str) -> dict:
    try:
        params = {"limit": limit, "offset": offset}
        if date_from:
            params["date_from"] = date_from
        if date_to:
            params["date_to"] = date_to
        resp = httpx.get(f"{API_BASE}/datasets/{dataset}/records", params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        st.error(f"Error fetching records: {exc}")
        return {"records": [], "total": 0}


# ── Sidebar ────────────────────────────────────────────────────────────────
dataset_names = fetch_dataset_names()
if not dataset_names:
    st.warning("No datasets available. Run the pipeline first.")
    st.stop()

selected = dataset_selector(dataset_names)
start_date, end_date = date_range_filter()
limit = row_limit_slider()

# ── Main content ──────────────────────────────────────────────────────────
result = fetch_records(
    selected,
    limit=limit,
    offset=0,
    date_from=str(start_date),
    date_to=str(end_date),
)
records = result.get("records", [])
total = result.get("total", 0)

st.subheader(f"{selected} — {total:,} total records")

if records:
    df = pd.DataFrame(records)
    st.dataframe(df, use_container_width=True, hide_index=True)

    # CSV download
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download as CSV",
        data=csv,
        file_name=f"{selected}_{start_date}_{end_date}.csv",
        mime="text/csv",
    )
else:
    st.info("No records found for the selected filters.")
