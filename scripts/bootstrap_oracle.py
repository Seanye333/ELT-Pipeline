"""
Bootstrap script: Create Oracle schema, audit tables, and target tables.
Run once before the first pipeline execution: python scripts/bootstrap_oracle.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.logging_config import configure_logging, get_logger
from src.config.settings import OracleSettings
from src.load.oracle_loader import OracleLoader

logger = get_logger(__name__)


DDL_STATEMENTS = [
    # Audit: pipeline runs
    """
    CREATE TABLE IF NOT EXISTS {schema}.PIPELINE_RUN (
        run_id          VARCHAR2(128)   NOT NULL,
        dag_id          VARCHAR2(128),
        start_time      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
        end_time        TIMESTAMP,
        status          VARCHAR2(32)    DEFAULT 'RUNNING',
        files_processed NUMBER          DEFAULT 0,
        rows_inserted   NUMBER          DEFAULT 0,
        rows_updated    NUMBER          DEFAULT 0,
        error_message   VARCHAR2(4000),
        CONSTRAINT pk_pipeline_run PRIMARY KEY (run_id)
    )
    """,
    # Audit: per-file manifest
    """
    CREATE TABLE IF NOT EXISTS {schema}.FILE_MANIFEST (
        manifest_id     NUMBER          GENERATED ALWAYS AS IDENTITY,
        run_id          VARCHAR2(128),
        file_name       VARCHAR2(512),
        file_id         VARCHAR2(256),
        checksum_md5    VARCHAR2(64),
        minio_path      VARCHAR2(1024),
        processed_at    TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
        row_count       NUMBER          DEFAULT 0,
        status          VARCHAR2(32)    DEFAULT 'PENDING',
        CONSTRAINT pk_file_manifest PRIMARY KEY (manifest_id),
        CONSTRAINT fk_manifest_run FOREIGN KEY (run_id)
            REFERENCES {schema}.PIPELINE_RUN(run_id)
    )
    """,
    # Main data table (generic; adapt columns to your specific data)
    """
    CREATE TABLE IF NOT EXISTS {schema}.ELT_DATA (
        data_id         NUMBER          GENERATED ALWAYS AS IDENTITY,
        source_file     VARCHAR2(512)   NOT NULL,
        row_id          NUMBER          NOT NULL,
        loaded_at       TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
        data_json       CLOB,
        CONSTRAINT pk_elt_data PRIMARY KEY (data_id),
        CONSTRAINT uq_elt_data UNIQUE (source_file, row_id)
    )
    """,
]

# Oracle 21c+ supports IF NOT EXISTS; older versions need this workaround
DDL_FALLBACK = """
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM all_tables
    WHERE owner = '{schema}' AND table_name = '{table}';

    IF v_count = 0 THEN
        EXECUTE IMMEDIATE '{create_sql}';
    END IF;
END;
"""


def bootstrap_oracle() -> None:
    configure_logging()
    config = OracleSettings()
    loader = OracleLoader(config)

    if not loader.check_connectivity():
        logger.error("Cannot connect to Oracle. Check ORACLE_HOST, user, password.")
        sys.exit(1)

    schema = config.schema
    for stmt in DDL_STATEMENTS:
        sql = stmt.format(schema=schema).strip()
        # Strip the IF NOT EXISTS clause for Oracle < 21c compatibility
        # For Oracle 21c+ you can use IF NOT EXISTS directly
        try:
            loader.execute_ddl(sql)
            logger.info("ddl_applied", table=sql.split("TABLE")[1].split("(")[0].strip()[:50])
        except Exception as exc:
            error_msg = str(exc)
            if "ORA-00955" in error_msg or "name is already used" in error_msg.lower():
                logger.info("table_already_exists_skipping")
            else:
                logger.error("ddl_failed", error=error_msg)
                raise

    logger.info("oracle_bootstrap_complete", schema=schema)


if __name__ == "__main__":
    bootstrap_oracle()
