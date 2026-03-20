"""
Custom Airflow Hook for Microsoft Graph API.
Stores credentials in an Airflow Connection (conn_id: graph_api_default).

Connection fields:
  - conn_type: HTTP
  - host: login.microsoftonline.com
  - login: <client_id>
  - password: <client_secret>
  - extra (JSON): {"tenant_id": "...", "user_id": "..."}
"""
from __future__ import annotations

import json

from airflow.hooks.base import BaseHook

from src.config.settings import GraphAPISettings
from src.extract.graph_client import GraphAPIClient
from src.extract.onedrive_scanner import FileMetadata, OneDriveScanner


class GraphAPIHook(BaseHook):
    conn_name_attr = "graph_api_conn_id"
    default_conn_name = "graph_api_default"
    conn_type = "http"
    hook_name = "Microsoft Graph API"

    def __init__(self, graph_api_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.graph_api_conn_id = graph_api_conn_id
        self._client: GraphAPIClient | None = None

    def _build_settings(self) -> GraphAPISettings:
        conn = self.get_connection(self.graph_api_conn_id)
        extra = json.loads(conn.extra or "{}")
        return GraphAPISettings(
            _env_file=None,
            GRAPH_CLIENT_ID=conn.login or "",
            GRAPH_CLIENT_SECRET=conn.password or "",
            GRAPH_TENANT_ID=extra.get("tenant_id", ""),
            GRAPH_ONEDRIVE_USER_ID=extra.get("user_id", ""),
        )

    def get_client(self) -> GraphAPIClient:
        if self._client is None:
            self._client = GraphAPIClient(self._build_settings())
        return self._client

    def get_scanner(self) -> OneDriveScanner:
        settings = self._build_settings()
        return OneDriveScanner(client=self.get_client(), config=settings)

    def scan_onedrive(self, folder_path: str | None = None) -> list[FileMetadata]:
        return self.get_scanner().scan(folder_path)

    def check_connectivity(self) -> bool:
        return self.get_client().check_connectivity()
