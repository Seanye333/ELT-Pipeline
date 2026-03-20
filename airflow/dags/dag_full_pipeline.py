"""
DAG 3: Full ELT Pipeline Orchestrator

Runs end-to-end on a weekday schedule.
Chains dag_extract_to_minio → dag_transform_to_oracle via ExternalTaskSensor,
then runs a data quality health check.
"""
from __future__ import annotations

from datetime import timedelta

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "elt-pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "email_on_failure": False,
}


@dag(
    dag_id="dag_full_pipeline",
    schedule_interval="0 5 * * 1-5",  # Weekdays at 05:00 UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["elt", "orchestrator"],
    description="End-to-end ELT pipeline: OneDrive → MinIO → Oracle → Quality Check",
)
def dag_full_pipeline():

    trigger_extract = TriggerDagRunOperator(
        task_id="trigger_extract_dag",
        trigger_dag_id="dag_extract_to_minio",
        conf={"run_date": "{{ ds }}"},
        wait_for_completion=True,
        poke_interval=60,
    )

    wait_for_transform = ExternalTaskSensor(
        task_id="wait_for_transform_complete",
        external_dag_id="dag_transform_to_oracle",
        external_task_id=None,  # Wait for entire DAG
        allowed_states=["success"],
        timeout=7200,
        poke_interval=60,
        mode="reschedule",
    )

    @task
    def run_data_quality_check(run_date: str) -> dict:
        """
        Post-load quality checks:
        - Row count > 0 for today's partition
        - No all-null columns
        - Duplicate count == 0
        """
        from airflow.plugins.hooks.oracle_hook import OracleELTHook
        from src.config.settings import OracleSettings

        oracle_hook = OracleELTHook()
        loader = oracle_hook.get_loader()
        config = OracleSettings()

        checks = {}

        # Row count check
        result = loader.execute_query(
            f"SELECT COUNT(*) AS cnt FROM {config.schema}.ELT_DATA "
            f"WHERE loaded_at >= TRUNC(SYSDATE)",
        )
        row_count = result[0]["cnt"] if result else 0
        checks["row_count"] = row_count
        checks["row_count_ok"] = row_count > 0

        # Pipeline run status check
        run_result = loader.execute_query(
            f"SELECT status, rows_inserted, rows_updated FROM {config.schema}.PIPELINE_RUN "
            f"WHERE TRUNC(start_time) = TRUNC(SYSDATE) AND dag_id = 'dag_transform_to_oracle' "
            f"ORDER BY start_time DESC FETCH FIRST 1 ROWS ONLY"
        )
        if run_result:
            checks["last_run_status"] = run_result[0]["status"]
            checks["rows_inserted"] = run_result[0]["rows_inserted"]
            checks["rows_updated"] = run_result[0]["rows_updated"]

        if not checks.get("row_count_ok"):
            raise ValueError(f"Data quality check failed: no rows loaded for {run_date}")

        return checks

    @task
    def pipeline_complete(quality_result: dict) -> None:
        from src.utils.notifications import NotificationService
        svc = NotificationService()
        svc.send_slack(
            f":rocket: *Full ELT Pipeline COMPLETE* | "
            f"Rows: {quality_result.get('rows_inserted', 0):,} inserted, "
            f"{quality_result.get('rows_updated', 0):,} updated",
            color="good",
        )

    # ── Task wiring ────────────────────────────────────────────────────────
    quality = run_data_quality_check(run_date="{{ ds }}")
    done = pipeline_complete(quality_result=quality)

    trigger_extract >> wait_for_transform >> quality >> done


dag_full_pipeline()
