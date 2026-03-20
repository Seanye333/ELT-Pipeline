"""Abstract base class for all file transformers."""
from __future__ import annotations

import io
from abc import ABC, abstractmethod

import pandas as pd


class BaseTransformer(ABC):
    """All transformers must implement transform()."""

    @abstractmethod
    def transform(self, source: bytes | str | io.BytesIO) -> pd.DataFrame:
        """Transform raw bytes / path into a clean DataFrame."""
        ...

    @staticmethod
    def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
        """Convert column names to lower-case snake_case."""
        df.columns = (
            df.columns.str.strip()
            .str.lower()
            .str.replace(r"[\s\-/]+", "_", regex=True)
            .str.replace(r"[^\w]", "", regex=True)
        )
        return df

    @staticmethod
    def drop_fully_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
        return df.dropna(how="all")

    @staticmethod
    def coerce_dates(df: pd.DataFrame, date_columns: list[str]) -> pd.DataFrame:
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df

    @staticmethod
    def strip_string_columns(df: pd.DataFrame) -> pd.DataFrame:
        str_cols = df.select_dtypes(include="object").columns
        df[str_cols] = df[str_cols].apply(lambda c: c.str.strip())
        return df
