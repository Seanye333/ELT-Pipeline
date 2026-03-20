"""Common sidebar filters for Streamlit pages."""
from __future__ import annotations

from datetime import date, timedelta

import streamlit as st


def date_range_filter(label: str = "Date Range") -> tuple[date, date]:
    """Render a date range picker in the sidebar and return (start, end)."""
    st.sidebar.subheader(label)
    default_end = date.today()
    default_start = default_end - timedelta(days=30)
    start = st.sidebar.date_input("From", value=default_start, key=f"{label}_start")
    end = st.sidebar.date_input("To", value=default_end, key=f"{label}_end")
    return start, end


def dataset_selector(datasets: list[str]) -> str:
    """Render a dataset selector dropdown in the sidebar."""
    return st.sidebar.selectbox("Dataset", options=datasets)


def row_limit_slider(default: int = 100, max_val: int = 5000) -> int:
    return st.sidebar.slider("Max rows", min_value=10, max_value=max_val, value=default, step=50)
