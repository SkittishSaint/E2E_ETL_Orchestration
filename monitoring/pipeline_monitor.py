"""
Monitoring helpers for Airflow pipeline runs.

The functions in this module write lightweight operational metrics to
CONTROL_DB.CONTROL.PIPELINE_RUN_METRICS. They are intentionally defensive:
monitoring failures are logged, but they should not hide the original pipeline
failure or turn a successful data load into a failed business run.
"""
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from etl.loaders.snowflake_loader import SnowflakeLoader

logger = logging.getLogger(__name__)

METRICS_TABLE = "CONTROL_DB.CONTROL.PIPELINE_RUN_METRICS"


def duration_seconds_from_context(context: Dict[str, Any]) -> Optional[float]:
    """Calculate elapsed seconds for the current task or DAG run context."""
    task_instance = context.get("task_instance") or context.get("ti")
    task_duration = getattr(task_instance, "duration", None)
    if task_duration is not None:
        return float(task_duration)

    dag_run = context.get("dag_run")
    start_date = getattr(dag_run, "start_date", None)
    if not start_date:
        return None

    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)

    return round((datetime.now(timezone.utc) - start_date).total_seconds(), 2)


def record_pipeline_metric(
    dag_id: str,
    task_id: str,
    run_id: str,
    run_type: str,
    status: str,
    rows_loaded: int = 0,
    duration_seconds: Optional[float] = None,
    error_message: Optional[str] = None,
) -> bool:
    """Persist one pipeline monitoring record to Snowflake."""
    sql = f"""
    INSERT INTO {METRICS_TABLE}
        (dag_id, task_id, run_id, run_type, status, rows_loaded, duration_seconds, error_message, recorded_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
    """
    params = (
        dag_id,
        task_id,
        run_id,
        run_type,
        status,
        int(rows_loaded or 0),
        duration_seconds,
        error_message[:5000] if error_message else None,
    )

    try:
        with SnowflakeLoader() as loader:
            loader.execute(sql, params)
        logger.info(
            "Recorded pipeline metric: dag=%s run_type=%s status=%s rows=%s",
            dag_id,
            run_type,
            status,
            rows_loaded,
        )
        return True
    except Exception:
        logger.exception("Unable to persist pipeline monitoring metric.")
        return False


def record_task_failure(context: Dict[str, Any]) -> bool:
    """Record task failure details from an Airflow callback context."""
    dag = context.get("dag")
    task_instance = context.get("task_instance") or context.get("ti")
    exception = context.get("exception")

    dag_id = getattr(dag, "dag_id", "unknown_dag")
    task_id = getattr(task_instance, "task_id", "unknown_task")
    run_id = context.get("run_id") or getattr(context.get("dag_run"), "run_id", "unknown_run")

    return record_pipeline_metric(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        run_type="task_failure",
        status="FAILED",
        rows_loaded=0,
        duration_seconds=duration_seconds_from_context(context),
        error_message=str(exception) if exception else "No exception details available.",
    )
