"""
Custom Airflow Hook for Oracle DB (python-oracledb thin mode).
Stores credentials in an Airflow Connection (conn_id: oracle_elt_default).

Connection fields:
  - conn_type: Oracle
  - host: <oracle-host>
  - port: 1521
  - login: <user>
  - password: <password>
  - schema: ELT_PIPELINE
  - extra (JSON): {"service_name": "XEPDB1"}
"""
from __future__ import annotations

import json

from airflow.hooks.base import BaseHook

from src.config.settings import OracleSettings
from src.load.oracle_loader import OracleLoader


class OracleELTHook(BaseHook):
    conn_name_attr = "oracle_conn_id"
    default_conn_name = "oracle_elt_default"
    conn_type = "oracle"
    hook_name = "Oracle ELT"

    def __init__(self, oracle_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.oracle_conn_id = oracle_conn_id
        self._loader: OracleLoader | None = None

    def _build_settings(self) -> OracleSettings:
        conn = self.get_connection(self.oracle_conn_id)
        extra = json.loads(conn.extra or "{}")
        return OracleSettings(
            _env_file=None,
            ORACLE_HOST=conn.host or "localhost",
            ORACLE_PORT=conn.port or 1521,
            ORACLE_USER=conn.login or "",
            ORACLE_PASSWORD=conn.password or "",
            ORACLE_SCHEMA=conn.schema or "ELT_PIPELINE",
            ORACLE_SERVICE_NAME=extra.get("service_name", "XEPDB1"),
        )

    def get_loader(self) -> OracleLoader:
        if self._loader is None:
            self._loader = OracleLoader(self._build_settings())
        return self._loader

    def check_connectivity(self) -> bool:
        return self.get_loader().check_connectivity()
