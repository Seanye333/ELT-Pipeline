"""
Transform CSV files into clean DataFrames.
Auto-detects encoding using chardet.
"""
from __future__ import annotations

import io

import chardet
import pandas as pd

from src.config.logging_config import get_logger
from src.transform.base_transformer import BaseTransformer

logger = get_logger(__name__)


class CSVTransformer(BaseTransformer):
    """
    Reads a CSV file and returns a clean, normalized DataFrame.

    Parameters
    ----------
    delimiter : str or None
        Column separator. If None, pandas sniffs it automatically.
    date_columns : list[str]
        Column names (after normalization) to coerce to datetime.
    encoding : str or None
        Force a specific encoding. If None, chardet auto-detects.
    """

    def __init__(
        self,
        delimiter: str | None = None,
        date_columns: list[str] | None = None,
        encoding: str | None = None,
    ) -> None:
        self.delimiter = delimiter
        self.date_columns = date_columns or []
        self.encoding = encoding

    def _detect_encoding(self, raw: bytes) -> str:
        detected = chardet.detect(raw[:65536])
        enc = detected.get("encoding") or "utf-8"
        logger.debug("encoding_detected", encoding=enc, confidence=detected.get("confidence"))
        return enc

    def transform(self, source: bytes | str | io.BytesIO) -> pd.DataFrame:
        if isinstance(source, str):
            with open(source, "rb") as f:
                raw = f.read()
        elif isinstance(source, io.BytesIO):
            raw = source.read()
            source.seek(0)
        else:
            raw = source

        encoding = self.encoding or self._detect_encoding(raw)
        buf = io.BytesIO(raw)

        logger.info("reading_csv", encoding=encoding, delimiter=self.delimiter)

        df = pd.read_csv(
            buf,
            sep=self.delimiter,
            encoding=encoding,
            engine="python" if self.delimiter is None else "c",
            on_bad_lines="warn",
        )

        df = self.normalize_column_names(df)
        df = self.drop_fully_empty_rows(df)
        df = self.strip_string_columns(df)
        if self.date_columns:
            df = self.coerce_dates(df, self.date_columns)

        logger.info("csv_transform_complete", rows=len(df), columns=list(df.columns))
        return df
