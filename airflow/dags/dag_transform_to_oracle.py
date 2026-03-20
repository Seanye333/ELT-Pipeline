"""
DAG 2: MinIO (Bronze) → Transform → MinIO (Silver) → Oracle DB

Triggered by dag_extract_to_minio or manually.
Tasks:
  1. read_manifest_from_minio
  2. transform_and_load (dynamic task map — one instance per file)
  3. record_pipeline_run
  4. send_completion_notification
"""
from __future__ import annotations

import json
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "elt-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
}


@dag(
    dag_id="dag_transform_to_oracle",
    schedule_interval=None,  # Triggered by dag_extract_to_minio
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["elt", "transform", "oracle", "minio"],
    description="Transform files from MinIO bronze, write silver Parquet, load into Oracle",
    params={
        "run_date": "",
        "manifest_key": "",
        "target_table": "ELT_DATA",
        "natural_keys": ["source_file", "row_id"],
    },
)
def dag_transform_to_oracle():

    @task
    def read_manifest(run_date: str, manifest_key: str) -> list[dict]:
        """Read the manifest JSON from MinIO and return list of upload records."""
        from airflow.plugins.hooks.minio_hook import MinIOHook
        from src.config.settings import MinIOSettings

        minio_hook = MinIOHook()
        client = minio_hook.get_client()
        config = MinIOSettings()

        key = manifest_key or f"{run_date}/_manifest.json"
        resp = client.client.get_object(Bucket=config.bronze_bucket, Key=key)
        manifest = json.loads(resp["Body"].read())
        return manifest.get("files", [])

    @task
    def transform_and_load(
        upload_record: dict,
        target_table: str,
        natural_keys: list[str],
        run_date: str,
    ) -> dict:
        """Transform one file from bronze → silver Parquet → Oracle MERGE."""
        from airflow.plugins.hooks.minio_hook import MinIOHook
        from airflow.plugins.hooks.oracle_hook import OracleELTHook
        from src.transform.csv_transformer import CSVTransformer
        from src.transform.excel_transformer import ExcelTransformer

        minio_hook = MinIOHook()
        oracle_hook = OracleELTHook()

        uploader = minio_hook.get_uploader()
        loader = oracle_hook.get_loader()

        minio_key = upload_record["minio_key"]
        file_name = upload_record["file_name"]

        # Download raw bytes from bronze
        raw_bytes = uploader.download_raw(minio_key)

        # Transform
        ext = "." + file_name.rsplit(".", 1)[-1].lower() if "." in file_name else ""
        if ext in (".xlsx", ".xls", ".xlsm", ".xlsb"):
            df = ExcelTransformer().transform(raw_bytes)
        else:
            df = CSVTransformer().transform(raw_bytes)

        # Add audit columns
        import pandas as pd
        from datetime import datetime, timezone
        df["source_file"] = file_name
        df["loaded_at"] = datetime.now(timezone.utc).isoformat()
        if "row_id" not in df.columns:
            df["row_id"] = range(len(df))

        # Write Parquet to silver
        silver_key = uploader.upload_parquet(df, file_name, run_date=run_date)

        # MERGE into Oracle
        rows_inserted, rows_updated = loader.upsert(df, target_table, natural_keys)

        return {
            "file_name": file_name,
            "silver_key": silver_key,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
        }

    @task
    def record_pipeline_run(load_results: list[dict], run_date: str, dag_run_id: str) -> None:
        from airflow.plugins.hooks.oracle_hook import OracleELTHook

        oracle_hook = OracleELTHook()
        loader = oracle_hook.get_loader()

        total_inserted = sum(r.get("rows_inserted", 0) for r in load_results)
        total_updated = sum(r.get("rows_updated", 0) for r in load_results)

        loader.record_pipeline_run(
            run_id=dag_run_id,
            dag_id="dag_transform_to_oracle",
            status="SUCCESS",
            files_processed=len(load_results),
            rows_inserted=total_inserted,
            rows_updated=total_updated,
        )

    @task
    def send_completion_notification(load_results: list[dict], run_date: str) -> None:
        from src.utils.notifications import NotificationService

        svc = NotificationService()
        total_rows = sum(r.get("rows_inserted", 0) + r.get("rows_updated", 0) for r in load_results)
        svc.notify_pipeline_success(
            run_id=run_date,
            rows=total_rows,
            files=len(load_results),
        )

    # ── Task wiring ────────────────────────────────────────────────────────
    run_date = "{{ params.run_date or ds }}"
    manifest_key = "{{ params.manifest_key }}"
    target_table = "{{ params.target_table }}"
    natural_keys_str = "{{ params.natural_keys }}"
    dag_run_id = "{{ run_id }}"

    files = read_manifest(run_date=run_date, manifest_key=manifest_key)

    results = transform_and_load.expand(
        upload_record=files,
        target_table=[target_table],
        natural_keys=[natural_keys_str],
        run_date=[run_date],
    )

    run_record = record_pipeline_run(
        load_results=results, run_date=run_date, dag_run_id=dag_run_id
    )
    notification = send_completion_notification(load_results=results, run_date=run_date)

    results >> [run_record, notification]


dag_transform_to_oracle()
