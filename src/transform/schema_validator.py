"""
Validate a DataFrame against an expected schema before loading to Oracle.
Raises typed exceptions with detailed column-level error messages.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from src.config.logging_config import get_logger

logger = get_logger(__name__)


class SchemaValidationError(Exception):
    """Raised when a DataFrame fails schema validation."""


@dataclass
class ColumnSpec:
    name: str
    dtype: str                # e.g. "object", "int64", "float64", "datetime64[ns]"
    nullable: bool = True
    max_length: int | None = None   # for string columns
    allowed_values: list[Any] = field(default_factory=list)


class SchemaValidator:
    """
    Validate a DataFrame against a list of ColumnSpec definitions.

    Usage:
        schema = [
            ColumnSpec("employee_id", "int64", nullable=False),
            ColumnSpec("name", "object", max_length=200),
            ColumnSpec("department", "object", allowed_values=["HR", "IT", "Finance"]),
        ]
        validator = SchemaValidator(schema)
        validator.validate(df)  # raises SchemaValidationError on failure
    """

    def __init__(self, schema: list[ColumnSpec]) -> None:
        self._schema = {col.name: col for col in schema}

    def validate(self, df: pd.DataFrame) -> None:
        """Validate the DataFrame. Raises SchemaValidationError on any failure."""
        errors: list[str] = []

        # Check required columns exist
        for col_name, spec in self._schema.items():
            if col_name not in df.columns:
                if not spec.nullable:
                    errors.append(f"Missing required column: '{col_name}'")
                continue

            series = df[col_name]

            # Null check
            if not spec.nullable and series.isna().any():
                null_count = series.isna().sum()
                errors.append(f"Column '{col_name}' has {null_count} null values (not allowed)")

            # Dtype check (lenient: only warn if explicitly wrong)
            if spec.dtype.startswith("int") and not pd.api.types.is_integer_dtype(series.dropna()):
                errors.append(f"Column '{col_name}' expected integer, got {series.dtype}")

            if spec.dtype.startswith("float") and not pd.api.types.is_float_dtype(series):
                errors.append(f"Column '{col_name}' expected float, got {series.dtype}")

            # Max length for string columns
            if spec.max_length and pd.api.types.is_string_dtype(series):
                too_long = series.dropna().str.len() > spec.max_length
                if too_long.any():
                    errors.append(
                        f"Column '{col_name}' has {too_long.sum()} values exceeding "
                        f"max length {spec.max_length}"
                    )

            # Allowed values check
            if spec.allowed_values:
                invalid = ~series.dropna().isin(spec.allowed_values)
                if invalid.any():
                    bad_vals = series[series.notna() & invalid].unique()[:5].tolist()
                    errors.append(
                        f"Column '{col_name}' has invalid values: {bad_vals}. "
                        f"Allowed: {spec.allowed_values}"
                    )

        if errors:
            msg = f"Schema validation failed with {len(errors)} error(s):\n" + "\n".join(
                f"  - {e}" for e in errors
            )
            logger.error("schema_validation_failed", errors=errors)
            raise SchemaValidationError(msg)

        logger.info("schema_validation_passed", columns=len(self._schema), rows=len(df))
