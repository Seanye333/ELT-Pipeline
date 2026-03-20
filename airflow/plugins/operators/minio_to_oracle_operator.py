"""
Custom operator: Read a file from MinIO bronze, transform it,
write Parquet to MinIO silver, then MERGE into Oracle.
"""
from __future__ import annotations

from airflow.models import BaseOperator


class MinIOToOracleOperator(BaseOperator):
    """
    Reads a raw file from MinIO, transforms it, and loads it into Oracle DB.

    Parameters
    ----------
    minio_key : str
        Bronze bucket object key.
    table : str
        Target Oracle table name (schema prefix NOT needed; set in loader config).
    natural_keys : list[str]
        Columns that uniquely identify a row for MERGE.
    run_date : str
        Date partition string.
    minio_conn_id / oracle_conn_id : str
        Airflow connection IDs.
    """

    template_fields = ("minio_key", "run_date")

    def __init__(
        self,
        minio_key: str,
        table: str,
        natural_keys: list[str],
        run_date: str,
        minio_conn_id: str = "minio_default",
        oracle_conn_id: str = "oracle_elt_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.minio_key = minio_key
        self.table = table
        self.natural_keys = natural_keys
        self.run_date = run_date
        self.minio_conn_id = minio_conn_id
        self.oracle_conn_id = oracle_conn_id

    def execute(self, context) -> dict:
        from airflow.plugins.hooks.minio_hook import MinIOHook
        from airflow.plugins.hooks.oracle_hook import OracleELTHook

        from src.transform.csv_transformer import CSVTransformer
        from src.transform.excel_transformer import ExcelTransformer

        minio_hook = MinIOHook(self.minio_conn_id)
        oracle_hook = OracleELTHook(self.oracle_conn_id)

        uploader = minio_hook.get_uploader()
        loader = oracle_hook.get_loader()

        # Download raw bytes from bronze
        raw_bytes = uploader.download_raw(self.minio_key)
        file_name = self.minio_key.split("/")[-1]

        # Choose transformer based on file extension
        ext = "." + file_name.rsplit(".", 1)[-1].lower() if "." in file_name else ""
        if ext in (".xlsx", ".xls", ".xlsm", ".xlsb"):
            df = ExcelTransformer().transform(raw_bytes)
        else:
            df = CSVTransformer().transform(raw_bytes)

        # Write transformed Parquet to silver bucket
        silver_key = uploader.upload_parquet(df, file_name, run_date=self.run_date)

        # Merge into Oracle
        rows_inserted, rows_updated = loader.upsert(df, self.table, self.natural_keys)

        return {
            "table": self.table,
            "silver_key": silver_key,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
        }
