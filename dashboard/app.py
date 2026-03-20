"""
Streamlit multi-page entry point.
Sets global page config and branding.
Run with: streamlit run dashboard/app.py
"""
import streamlit as st

st.set_page_config(
    page_title="ELT Pipeline Dashboard",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.sidebar.title("ELT Pipeline")
st.sidebar.caption("Data Platform · Powered by Airflow + Oracle + MinIO")
st.sidebar.divider()

st.title("Welcome to the ELT Pipeline Dashboard")
st.markdown(
    """
    Navigate using the sidebar pages:

    | Page | Description |
    |---|---|
    | **Overview** | KPIs, row trends, dataset status |
    | **Data Explorer** | Browse Oracle tables interactively |
    | **Pipeline Health** | Run history, durations, error logs |

    ---
    Use the **FastAPI** at `http://localhost:8000/docs` for programmatic access.
    """
)
