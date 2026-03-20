"""Unit tests for SchemaValidator."""
from __future__ import annotations

import pandas as pd
import pytest

from src.transform.schema_validator import ColumnSpec, SchemaValidator, SchemaValidationError


class TestSchemaValidator:
    def test_passes_valid_dataframe(self, sample_dataframe):
        schema = [
            ColumnSpec("employee_id", "int64", nullable=False),
            ColumnSpec("name", "object", nullable=False),
            ColumnSpec("department", "object", allowed_values=["HR", "IT", "Finance"]),
        ]
        validator = SchemaValidator(schema)
        validator.validate(sample_dataframe)  # Should not raise

    def test_raises_on_missing_required_column(self, sample_dataframe):
        schema = [ColumnSpec("nonexistent_column", "object", nullable=False)]
        validator = SchemaValidator(schema)
        with pytest.raises(SchemaValidationError, match="Missing required column"):
            validator.validate(sample_dataframe)

    def test_raises_on_null_in_non_nullable_column(self):
        df = pd.DataFrame({"id": [1, None, 3], "name": ["a", "b", "c"]})
        schema = [ColumnSpec("id", "int64", nullable=False)]
        validator = SchemaValidator(schema)
        with pytest.raises(SchemaValidationError, match="null values"):
            validator.validate(df)

    def test_raises_on_invalid_allowed_value(self):
        df = pd.DataFrame({"status": ["ACTIVE", "UNKNOWN", "INACTIVE"]})
        schema = [ColumnSpec("status", "object", allowed_values=["ACTIVE", "INACTIVE"])]
        validator = SchemaValidator(schema)
        with pytest.raises(SchemaValidationError, match="invalid values"):
            validator.validate(df)

    def test_raises_on_string_too_long(self):
        df = pd.DataFrame({"code": ["A" * 10, "B" * 200]})
        schema = [ColumnSpec("code", "object", max_length=50)]
        validator = SchemaValidator(schema)
        with pytest.raises(SchemaValidationError, match="max length"):
            validator.validate(df)
