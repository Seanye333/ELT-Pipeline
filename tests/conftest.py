"""Shared pytest fixtures for unit and integration tests."""
from __future__ import annotations

import io
from unittest.mock import MagicMock

import pandas as pd
import pytest


# ── Graph API fixtures ────────────────────────────────────────────────────

@pytest.fixture
def mock_graph_client():
    """Mock GraphAPIClient that returns dummy file metadata."""
    client = MagicMock()
    client.check_connectivity.return_value = True
    client.get_paginated.return_value = [
        {
            "id": "file-001",
            "name": "sales_jan_2024.xlsx",
            "size": 2048,
            "lastModifiedDateTime": "2024-01-15T10:00:00Z",
            "file": {"mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
            "parentReference": {"path": "/Reports"},
        },
        {
            "id": "file-002",
            "name": "inventory_2024.csv",
            "size": 1024,
            "lastModifiedDateTime": "2024-01-15T11:00:00Z",
            "file": {"mimeType": "text/csv"},
            "parentReference": {"path": "/Reports"},
        },
    ]
    return client


# ── MinIO fixtures ────────────────────────────────────────────────────────

@pytest.fixture
def mock_minio_client():
    """Mock MinIOClient."""
    client = MagicMock()
    client.check_connectivity.return_value = True
    client.object_exists.return_value = False
    client.get_object_metadata.return_value = {}
    return client


# ── Oracle fixtures ───────────────────────────────────────────────────────

@pytest.fixture
def mock_oracle_loader():
    """Mock OracleLoader."""
    loader = MagicMock()
    loader.check_connectivity.return_value = True
    loader.upsert.return_value = (100, 0)
    loader.bulk_insert.return_value = 100
    return loader


# ── DataFrame fixtures ────────────────────────────────────────────────────

@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    return pd.DataFrame({
        "employee_id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "department": ["HR", "IT", "Finance"],
        "salary": [50000.0, 60000.0, 55000.0],
        "hire_date": pd.to_datetime(["2022-01-01", "2022-06-15", "2023-03-01"]),
    })


@pytest.fixture
def sample_excel_bytes(sample_dataframe) -> bytes:
    """Minimal in-memory Excel file from the sample DataFrame."""
    buf = io.BytesIO()
    sample_dataframe.to_excel(buf, index=False)
    buf.seek(0)
    return buf.read()


@pytest.fixture
def sample_csv_bytes(sample_dataframe) -> bytes:
    return sample_dataframe.to_csv(index=False).encode("utf-8")
