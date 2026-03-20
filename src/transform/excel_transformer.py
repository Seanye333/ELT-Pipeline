"""
Transform Excel files (.xlsx / .xls / .xlsm) into clean DataFrames.
Handles merged cells, multi-row headers, and date columns.
"""
from __future__ import annotations

import io

import pandas as pd

from src.config.logging_config import get_logger
from src.transform.base_transformer import BaseTransformer

logger = get_logger(__name__)


class ExcelTransformer(BaseTransformer):
    """
    Reads an Excel file and returns a clean, normalized DataFrame.

    Parameters
    ----------
    sheet_name : int or str
        Sheet to read. Defaults to 0 (first sheet).
    header_row : int
        0-based row index for the header. Defaults to 0.
    date_columns : list[str]
        Column names (after normalization) to coerce to datetime.
    skip_rows : int
        Number of rows to skip before the header.
    """

    def __init__(
        self,
        sheet_name: int | str = 0,
        header_row: int = 0,
        date_columns: list[str] | None = None,
        skip_rows: int = 0,
    ) -> None:
        self.sheet_name = sheet_name
        self.header_row = header_row
        self.date_columns = date_columns or []
        self.skip_rows = skip_rows

    def transform(self, source: bytes | str | io.BytesIO) -> pd.DataFrame:
        if isinstance(source, bytes):
            source = io.BytesIO(source)

        logger.info("reading_excel", sheet=self.sheet_name, header_row=self.header_row)

        df = pd.read_excel(
            source,
            sheet_name=self.sheet_name,
            header=self.header_row,
            skiprows=self.skip_rows,
            engine="openpyxl",
        )

        df = self.normalize_column_names(df)
        df = self.drop_fully_empty_rows(df)
        df = self.strip_string_columns(df)
        if self.date_columns:
            df = self.coerce_dates(df, self.date_columns)

        logger.info("excel_transform_complete", rows=len(df), columns=list(df.columns))
        return df

    def transform_all_sheets(
        self, source: bytes | str | io.BytesIO
    ) -> dict[str, pd.DataFrame]:
        """Read all sheets and return a dict of {sheet_name: DataFrame}."""
        if isinstance(source, bytes):
            source = io.BytesIO(source)

        all_sheets = pd.read_excel(source, sheet_name=None, engine="openpyxl")
        result = {}
        for name, df in all_sheets.items():
            df = self.normalize_column_names(df)
            df = self.drop_fully_empty_rows(df)
            df = self.strip_string_columns(df)
            result[name] = df
        return result
