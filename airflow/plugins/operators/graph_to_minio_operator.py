"""
Custom operator: Download a single file from OneDrive and upload to MinIO bronze.
Designed for dynamic task mapping in dag_extract_to_minio.
"""
from __future__ import annotations

from airflow.models import BaseOperator

from src.extract.file_downloader import FileDownloader
from src.extract.onedrive_scanner import FileMetadata
from src.load.minio_uploader import MinIOUploader


class GraphToMinIOOperator(BaseOperator):
    """
    Downloads a file from OneDrive (via Graph API) and uploads it to MinIO bronze bucket.

    Parameters
    ----------
    file_meta_dict : dict
        Serialized FileMetadata dict (from XCom).
    run_date : str
        Date string (YYYY-MM-DD) used to partition the MinIO key.
    graph_api_conn_id : str
        Airflow connection ID for Graph API.
    minio_conn_id : str
        Airflow connection ID for MinIO.
    """

    template_fields = ("run_date",)

    def __init__(
        self,
        file_meta_dict: dict,
        run_date: str,
        graph_api_conn_id: str = "graph_api_default",
        minio_conn_id: str = "minio_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.file_meta_dict = file_meta_dict
        self.run_date = run_date
        self.graph_api_conn_id = graph_api_conn_id
        self.minio_conn_id = minio_conn_id

    def execute(self, context) -> dict:
        from airflow.plugins.hooks.graph_api_hook import GraphAPIHook
        from airflow.plugins.hooks.minio_hook import MinIOHook

        dag_run_id = context["dag_run"].run_id

        graph_hook = GraphAPIHook(self.graph_api_conn_id)
        minio_hook = MinIOHook(self.minio_conn_id)

        file_meta = FileMetadata(**self.file_meta_dict)
        downloader = FileDownloader(client=graph_hook.get_client())
        uploader = MinIOUploader(client=minio_hook.get_client())

        _, raw_bytes = downloader.download(file_meta)
        minio_key = uploader.upload_raw(
            file_meta,
            raw_bytes,
            run_date=self.run_date,
            dag_run_id=dag_run_id,
        )

        return {
            "file_name": file_meta.name,
            "minio_key": minio_key,
            "size_bytes": len(raw_bytes),
        }
