"""Reusable Plotly chart factory functions for the Streamlit dashboard."""
from __future__ import annotations

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd


def rows_over_time(df: pd.DataFrame, date_col: str = "date", value_col: str = "rows") -> go.Figure:
    """Line chart of rows loaded per day."""
    fig = px.line(
        df,
        x=date_col,
        y=value_col,
        markers=True,
        title="Rows Loaded Per Day",
        labels={date_col: "Date", value_col: "Rows"},
    )
    fig.update_layout(hovermode="x unified")
    return fig


def pipeline_status_pie(success: int, failed: int, running: int) -> go.Figure:
    """Pie chart of pipeline run statuses."""
    labels = ["Success", "Failed", "Running"]
    values = [success, failed, running]
    colors = ["#2ecc71", "#e74c3c", "#f39c12"]
    fig = go.Figure(
        go.Pie(
            labels=labels,
            values=values,
            marker_colors=colors,
            hole=0.4,
        )
    )
    fig.update_layout(title="Pipeline Run Status (Last 30 Days)")
    return fig


def duration_histogram(df: pd.DataFrame, duration_col: str = "duration_minutes") -> go.Figure:
    """Histogram of pipeline run durations in minutes."""
    fig = px.histogram(
        df,
        x=duration_col,
        nbins=20,
        title="Pipeline Duration Distribution",
        labels={duration_col: "Duration (minutes)"},
    )
    return fig


def kpi_gauge(value: float, title: str, max_val: float = 100) -> go.Figure:
    """Gauge chart for a single KPI metric."""
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=value,
            title={"text": title},
            gauge={
                "axis": {"range": [0, max_val]},
                "bar": {"color": "#3498db"},
                "steps": [
                    {"range": [0, max_val * 0.5], "color": "#e74c3c"},
                    {"range": [max_val * 0.5, max_val * 0.8], "color": "#f39c12"},
                    {"range": [max_val * 0.8, max_val], "color": "#2ecc71"},
                ],
            },
        )
    )
    fig.update_layout(height=250)
    return fig
