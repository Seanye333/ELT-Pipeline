"""Unit tests for ExcelTransformer."""
from __future__ import annotations

import io

import pandas as pd
import pytest

from src.transform.excel_transformer import ExcelTransformer


def make_excel_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    return buf.read()


class TestExcelTransformer:
    def test_basic_transform(self):
        df_input = pd.DataFrame({
            "Employee ID": [1, 2],
            "  Name  ": ["Alice ", " Bob"],
            "Salary": [50000, 60000],
        })
        transformer = ExcelTransformer()
        result = transformer.transform(make_excel_bytes(df_input))

        # Columns normalized to snake_case
        assert "employee_id" in result.columns
        assert "name" in result.columns

        # Strings stripped
        assert result["name"].iloc[0] == "Alice"
        assert result["name"].iloc[1] == "Bob"

    def test_drops_fully_empty_rows(self):
        df_input = pd.DataFrame({
            "id": [1, None, 2],
            "value": ["a", None, "c"],
        })
        transformer = ExcelTransformer()
        result = transformer.transform(make_excel_bytes(df_input))
        # Fully empty rows removed
        assert len(result) == 2

    def test_date_coercion(self):
        df_input = pd.DataFrame({
            "id": [1],
            "date_col": ["2024-01-15"],
        })
        transformer = ExcelTransformer(date_columns=["date_col"])
        result = transformer.transform(make_excel_bytes(df_input))
        assert pd.api.types.is_datetime64_any_dtype(result["date_col"])

    def test_accepts_bytes_io(self, sample_excel_bytes):
        buf = io.BytesIO(sample_excel_bytes)
        transformer = ExcelTransformer()
        result = transformer.transform(buf)
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
