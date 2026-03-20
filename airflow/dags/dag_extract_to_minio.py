"""
DAG 1: OneDrive → MinIO (Bronze Layer)

Schedule: Daily at 06:00 UTC
Tasks:
  1. check_graph_api_connectivity
  2. scan_onedrive_for_files
  3. [branch] skip if no files found
  4. download_and_upload (dynamic task map — one instance per file)
  5. write_manifest_to_minio
  6. trigger_transform_dag
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "elt-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    dag_id="dag_extract_to_minio",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["elt", "extract", "onedrive", "minio"],
    description="Extract files from OneDrive and store raw copies in MinIO bronze bucket",
)
def dag_extract_to_minio():

    @task
    def check_graph_api_connectivity() -> bool:
        from airflow.plugins.hooks.graph_api_hook import GraphAPIHook
        hook = GraphAPIHook()
        ok = hook.check_connectivity()
        if not ok:
            raise ConnectionError("Cannot reach Microsoft Graph API. Check credentials.")
        return True

    @task
    def scan_onedrive_for_files(folder_path: str | None = None) -> list[dict]:
        from airflow.plugins.hooks.graph_api_hook import GraphAPIHook
        hook = GraphAPIHook()
        folder = folder_path or Variable.get("onedrive_folder_path", default_var=None)
        files = hook.scan_onedrive(folder)
        return [f.to_dict() for f in files]

    @task.branch
    def check_files_found(file_dicts: list[dict]) -> str:
        if not file_dicts:
            return "no_files_found"
        return "download_and_upload"

    @task
    def download_and_upload(file_meta_dict: dict, run_date: str) -> dict:
        from airflow.plugins.hooks.graph_api_hook import GraphAPIHook
        from airflow.plugins.hooks.minio_hook import MinIOHook
        from src.extract.file_downloader import FileDownloader
        from src.extract.onedrive_scanner import FileMetadata
        from src.load.minio_uploader import MinIOUploader

        file_meta = FileMetadata(**file_meta_dict)
        graph_hook = GraphAPIHook()
        minio_hook = MinIOHook()

        downloader = FileDownloader(client=graph_hook.get_client())
        uploader = MinIOUploader(client=minio_hook.get_client())

        _, raw_bytes = downloader.download(file_meta)
        minio_key = uploader.upload_raw(file_meta, raw_bytes, run_date=run_date)

        return {"file_name": file_meta.name, "minio_key": minio_key, "size_bytes": len(raw_bytes)}

    @task
    def write_manifest(upload_results: list[dict], run_date: str) -> str:
        """Write a JSON manifest of uploaded files to MinIO bronze bucket."""
        import io
        from datetime import datetime, timezone
        from airflow.plugins.hooks.minio_hook import MinIOHook
        from src.config.settings import MinIOSettings

        minio_hook = MinIOHook()
        client = minio_hook.get_client()
        config = MinIOSettings()

        manifest = {
            "run_date": run_date,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "file_count": len(upload_results),
            "files": upload_results,
        }
        manifest_bytes = json.dumps(manifest, indent=2).encode()
        manifest_key = f"{run_date}/_manifest.json"

        client.client.put_object(
            Bucket=config.bronze_bucket,
            Key=manifest_key,
            Body=manifest_bytes,
            ContentType="application/json",
        )
        return manifest_key

    # ── Task wiring ────────────────────────────────────────────────────────
    run_date = "{{ ds }}"  # Airflow execution date as YYYY-MM-DD

    connectivity_ok = check_graph_api_connectivity()
    file_dicts = scan_onedrive_for_files()
    branch = check_files_found(file_dicts)

    no_files = EmptyOperator(task_id="no_files_found")
    upload_results = download_and_upload.expand(
        file_meta_dict=file_dicts,
        run_date=[run_date],  # Same date for all mapped instances
    )

    manifest_key = write_manifest(upload_results=upload_results, run_date=run_date)

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id="dag_transform_to_oracle",
        conf={"run_date": run_date, "manifest_key": "{{ ti.xcom_pull(task_ids='write_manifest') }}"},
        wait_for_completion=False,
    )

    # Dependencies
    connectivity_ok >> file_dicts >> branch
    branch >> [no_files, upload_results]
    upload_results >> manifest_key >> trigger_transform


dag_extract_to_minio()
