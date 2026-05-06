"""
config/dag_config.py
────────────────────
Shared DAG default arguments and alert callbacks.
"""
import os
from datetime import datetime, timedelta
from airflow.utils.email import send_email


# ── Alert callback ────────────────────────────────────────────────────────
def on_failure_callback(context: dict) -> None:
    """Send an email alert on task failure."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exec_date = context["execution_date"]
    log_url = context["task_instance"].log_url
    exception = context.get("exception", "No exception details")

    subject = f"[AIRFLOW FAILURE] {dag_id} › {task_id}"
    body = f"""
    <h3>Task Failure Alert</h3>
    <table>
      <tr><td><b>DAG</b></td><td>{dag_id}</td></tr>
      <tr><td><b>Task</b></td><td>{task_id}</td></tr>
      <tr><td><b>Execution Date</b></td><td>{exec_date}</td></tr>
      <tr><td><b>Exception</b></td><td>{exception}</td></tr>
    </table>
    <p><a href="{log_url}">View Logs</a></p>
    """
    recipients = os.environ.get("ALERT_RECIPIENTS", "").split(",")
    if recipients:
        send_email(to=recipients, subject=subject, html_content=body)


def on_success_callback(context: dict) -> None:
    """Optional: log pipeline completion to a monitoring table."""
    pass  # Extended in monitoring/pipeline_monitor.py


# ── Default args shared across all DAGs ──────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "on_failure_callback": on_failure_callback,
    "execution_timeout": timedelta(hours=2),
}

# ── Scheduling constants ──────────────────────────────────────────────────
DAILY_SCHEDULE = "0 2 * * *"        # 02:00 UTC daily
HOURLY_SCHEDULE = "0 * * * *"       # top of every hour
QUALITY_SCHEDULE = "30 2 * * *"     # 02:30 UTC daily (after load)

# ── Pipeline metadata ─────────────────────────────────────────────────────
PIPELINE_START_DATE = datetime(2024, 1, 1)
CATCHUP = False
MAX_ACTIVE_RUNS = 1
TAGS = ["etl", "snowflake", "mca-project"]
