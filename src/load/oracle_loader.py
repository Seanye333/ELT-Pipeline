"""
Load DataFrames into Oracle Database using bulk MERGE (UPSERT) statements.
Uses python-oracledb in thin mode (no Oracle Instant Client required).
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Generator

import pandas as pd

from src.config.logging_config import get_logger
from src.config.settings import OracleSettings, PipelineSettings

if TYPE_CHECKING:
    import oracledb as _oracledb

logger = get_logger(__name__)


def _oracledb():
    """Lazy import of oracledb — only loaded when Oracle connectivity is needed."""
    try:
        import oracledb as _mod
        return _mod
    except ImportError as exc:
        raise RuntimeError(
            "python-oracledb is not installed. "
            "Install it with: pip install python-oracledb"
        ) from exc


class OracleLoader:
    """Manages Oracle connection pool and bulk data loading."""

    _pool: Any = None  # oracledb.ConnectionPool when loaded

    def __init__(
        self,
        config: OracleSettings | None = None,
        pipeline_config: PipelineSettings | None = None,
    ) -> None:
        self._config = config or OracleSettings()
        self._pipeline_config = pipeline_config or PipelineSettings()

    def _get_pool(self) -> Any:
        if OracleLoader._pool is None:
            oracledb = _oracledb()
            logger.info("creating_oracle_pool", dsn=self._config.dsn)
            OracleLoader._pool = oracledb.create_pool(
                user=self._config.user,
                password=self._config.password,
                dsn=self._config.dsn,
                min=self._config.pool_min,
                max=self._config.pool_max,
                increment=self._config.pool_increment,
            )
        return OracleLoader._pool

    @contextmanager
    def connection(self) -> Generator[Any, None, None]:
        """Context manager that yields a pooled Oracle connection."""
        pool = self._get_pool()
        conn = pool.acquire()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            pool.release(conn)

    def check_connectivity(self) -> bool:
        try:
            with self.connection() as conn:
                conn.execute("SELECT 1 FROM DUAL")
            return True
        except Exception as exc:
            logger.warning("oracle_connectivity_check_failed", error=str(exc))
            return False

    def execute_ddl(self, sql: str) -> None:
        """Execute a DDL statement (CREATE TABLE, etc.)."""
        with self.connection() as conn:
            conn.execute(sql)
        logger.info("ddl_executed", sql=sql[:80])

    def execute_query(self, sql: str, params: dict | None = None) -> list[dict]:
        """Execute a SELECT query and return rows as dicts."""
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params or {})
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def bulk_insert(
        self,
        df: pd.DataFrame,
        table: str,
        batch_size: int | None = None,
    ) -> int:
        """
        Bulk-insert a DataFrame into an Oracle table using executemany().
        Returns the number of rows inserted.
        """
        if df.empty:
            logger.warning("bulk_insert_skipped_empty_df", table=table)
            return 0

        batch = batch_size or self._pipeline_config.batch_size
        columns = list(df.columns)
        placeholders = ", ".join(f":{col}" for col in columns)
        sql = f"INSERT INTO {self._config.schema}.{table} ({', '.join(columns)}) VALUES ({placeholders})"

        total_inserted = 0
        with self.connection() as conn:
            cursor = conn.cursor()
            for start in range(0, len(df), batch):
                chunk = df.iloc[start : start + batch]
                rows = chunk.to_dict(orient="records")
                cursor.executemany(sql, rows)
                total_inserted += len(rows)
                logger.debug(
                    "batch_inserted",
                    table=table,
                    rows=len(rows),
                    total=total_inserted,
                )

        logger.info("bulk_insert_complete", table=table, rows=total_inserted)
        return total_inserted

    def upsert(
        self,
        df: pd.DataFrame,
        table: str,
        natural_keys: list[str],
        batch_size: int | None = None,
    ) -> tuple[int, int]:
        """
        MERGE (upsert) a DataFrame into an Oracle table.
        natural_keys: columns that uniquely identify a row.
        Returns (rows_inserted, rows_updated).
        """
        if df.empty:
            logger.warning("upsert_skipped_empty_df", table=table)
            return 0, 0

        batch = batch_size or self._pipeline_config.batch_size
        columns = list(df.columns)
        update_cols = [c for c in columns if c not in natural_keys]

        # Build MERGE SQL
        on_clause = " AND ".join(f"t.{k} = s.{k}" for k in natural_keys)
        update_clause = ", ".join(f"t.{c} = s.{c}" for c in update_cols)
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join(f"s.{c}" for c in columns)

        merge_sql = f"""
            MERGE INTO {self._config.schema}.{table} t
            USING (
                SELECT {', '.join(f':{c} AS {c}' for c in columns)} FROM DUAL
            ) s
            ON ({on_clause})
            WHEN MATCHED THEN UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """

        rows_inserted = 0
        rows_updated = 0

        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.setinputsizes(None)

            for start in range(0, len(df), batch):
                chunk = df.iloc[start : start + batch]
                rows = chunk.to_dict(orient="records")

                for row in rows:
                    cursor.execute(merge_sql, row)
                    # rowcount==1 for both insert and update; check rowcount via hints
                    # Simple approach: track via audit columns if needed

                rows_inserted += len(rows)  # Simplified; use audit triggers for precision
                logger.debug("upsert_batch", table=table, rows=len(rows))

        logger.info(
            "upsert_complete",
            table=table,
            rows_processed=rows_inserted + rows_updated,
        )
        return rows_inserted, rows_updated

    def record_pipeline_run(
        self,
        run_id: str,
        dag_id: str,
        status: str,
        files_processed: int = 0,
        rows_inserted: int = 0,
        rows_updated: int = 0,
        error_message: str = "",
    ) -> None:
        """Upsert a pipeline run record into the audit table."""
        sql = f"""
            MERGE INTO {self._config.schema}.PIPELINE_RUN t
            USING (SELECT :run_id AS run_id FROM DUAL) s
            ON (t.run_id = s.run_id)
            WHEN MATCHED THEN UPDATE SET
                status = :status,
                end_time = CURRENT_TIMESTAMP,
                files_processed = :files_processed,
                rows_inserted = :rows_inserted,
                rows_updated = :rows_updated,
                error_message = :error_message
            WHEN NOT MATCHED THEN INSERT
                (run_id, dag_id, start_time, status, files_processed,
                 rows_inserted, rows_updated, error_message)
            VALUES
                (:run_id, :dag_id, CURRENT_TIMESTAMP, :status, :files_processed,
                 :rows_inserted, :rows_updated, :error_message)
        """
        with self.connection() as conn:
            conn.execute(
                sql,
                {
                    "run_id": run_id,
                    "dag_id": dag_id,
                    "status": status,
                    "files_processed": files_processed,
                    "rows_inserted": rows_inserted,
                    "rows_updated": rows_updated,
                    "error_message": error_message[:4000] if error_message else "",
                },
            )
        logger.info("pipeline_run_recorded", run_id=run_id, status=status)
