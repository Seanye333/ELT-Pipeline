"""Unit tests for CSVTransformer."""
from __future__ import annotations

import pandas as pd
import pytest

from src.transform.csv_transformer import CSVTransformer


class TestCSVTransformer:
    def test_basic_transform(self, sample_csv_bytes):
        transformer = CSVTransformer()
        result = transformer.transform(sample_csv_bytes)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3

    def test_column_normalization(self):
        csv = b"Employee ID,Full Name,Hire Date\n1,Alice,2024-01-01\n"
        transformer = CSVTransformer()
        result = transformer.transform(csv)
        assert "employee_id" in result.columns
        assert "full_name" in result.columns

    def test_utf8_encoding(self):
        csv = "name,city\nJosé,São Paulo\n".encode("utf-8")
        transformer = CSVTransformer(encoding="utf-8")
        result = transformer.transform(csv)
        assert result["name"].iloc[0] == "José"

    def test_custom_delimiter(self):
        tsv = b"col_a\tcol_b\n1\t2\n3\t4\n"
        transformer = CSVTransformer(delimiter="\t")
        result = transformer.transform(tsv)
        assert list(result.columns) == ["col_a", "col_b"]
        assert len(result) == 2
