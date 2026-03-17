from __future__ import annotations

from datetime import datetime, timedelta, timezone
import subprocess
import uuid

from airflow.sdk import DAG, task, Param, get_current_context
from airflow.providers.ssh.operators.ssh import SSHOperator


DAG_ID = "open311_s3_pipeline"

# Update these only if your paths differ
LOCAL_ENV_FILE = "/home/sxi219/env.sh"
SSH_CONN_ID = "spark_remote_ssh"


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _run_local_bash(command: str) -> None:
    subprocess.run(["bash", "-lc", command], check=True)


with DAG(
    dag_id=DAG_ID,
    description="Open311 raw->bronze->silver locally, aggregate->baseline remotely via SSH",
    start_date=datetime(2026, 3, 17, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["urban-stress", "open311", "s3", "spark", "ssh"],
    params={
        "city": Param("boston", type="string"),
        "window_hours": Param(1, type="integer"),
        "lookback_hours": Param(168, type="integer"),
        "min_history_points": Param(24, type="integer"),
    },
) as dag:

    @task(task_id="prepare_run_context")
    def prepare_run_context() -> dict:
        context = get_current_context()
        dag_run = context["dag_run"]
        params = context["params"]
        conf = dag_run.conf or {}

        city = conf.get("city", params["city"])
        window_hours = int(conf.get("window_hours", params["window_hours"]))
        lookback_hours = int(conf.get("lookback_hours", params["lookback_hours"]))
        min_history_points = int(conf.get("min_history_points", params["min_history_points"]))

        updated_before_dt = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        updated_after_dt = updated_before_dt - timedelta(hours=window_hours)

        batch_id = updated_before_dt.strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
        ingest_date = updated_before_dt.strftime("%Y-%m-%d")

        return {
            "city": city,
            "updated_after": _iso_z(updated_after_dt),
            "updated_before": _iso_z(updated_before_dt),
            "batch_id": batch_id,
            "ingest_date": ingest_date,
            "lookback_hours": lookback_hours,
            "min_history_points": min_history_points,
        }

    @task(task_id="run_ingest_local")
    def run_ingest_local(run_context: dict) -> None:
        cmd = f"""
        set -euo pipefail
        source "{LOCAL_ENV_FILE}"
        cd "$LOCAL_REPO_ROOT"
        bash scripts/open311/run_ingest.sh \
          --city "{run_context['city']}" \
          --updated-after "{run_context['updated_after']}" \
          --updated-before "{run_context['updated_before']}" \
          --bucket "$URBAN_STRESS_BUCKET" \
          --batch-id "{run_context['batch_id']}" \
          --env-file "$LOCAL_ENV_FILE" \
          --python-bin "$LOCAL_PYTHON_BIN"
        """
        _run_local_bash(cmd)

    @task(task_id="run_bronze_local")
    def run_bronze_local(run_context: dict) -> None:
        cmd = f"""
        set -euo pipefail
        source "{LOCAL_ENV_FILE}"
        cd "$LOCAL_REPO_ROOT"
        bash scripts/open311/run_bronze.sh \
          --bucket "$URBAN_STRESS_BUCKET" \
          --city "{run_context['city']}" \
          --ingest-date "{run_context['ingest_date']}" \
          --batch-id "{run_context['batch_id']}" \
          --env-file "$LOCAL_ENV_FILE" \
          --python-bin "$LOCAL_PYTHON_BIN"
        """
        _run_local_bash(cmd)

    @task(task_id="run_silver_local")
    def run_silver_local(run_context: dict) -> None:
        cmd = f"""
        set -euo pipefail
        source "{LOCAL_ENV_FILE}"
        cd "$LOCAL_REPO_ROOT"
        bash scripts/open311/run_silver.sh \
          --bucket "$URBAN_STRESS_BUCKET" \
          --city "{run_context['city']}" \
          --ingest-date "{run_context['ingest_date']}" \
          --batch-id "{run_context['batch_id']}" \
          --mapping-file "$LOCAL_REPO_ROOT/etl/open311/category_mapping.csv" \
          --env-file "$LOCAL_ENV_FILE" \
          --python-bin "$LOCAL_PYTHON_BIN"
        """
        _run_local_bash(cmd)

    run_aggregate_remote = SSHOperator(
        task_id="run_aggregate_remote",
        ssh_conn_id=SSH_CONN_ID,
        command="""
        set -euo pipefail
        source "$REMOTE_ENV_FILE"
        cd "$REMOTE_REPO_ROOT"
        bash scripts/open311/run_aggregate.sh \
          --bucket "$URBAN_STRESS_BUCKET" \
          --city "{{ ti.xcom_pull(task_ids='prepare_run_context')['city'] }}" \
          --ingest-date "{{ ti.xcom_pull(task_ids='prepare_run_context')['ingest_date'] }}" \
          --batch-id "{{ ti.xcom_pull(task_ids='prepare_run_context')['batch_id'] }}" \
          --env-file "$REMOTE_ENV_FILE" \
          --spark-submit-bin "$REMOTE_SPARK_SUBMIT_BIN" \
          --spark-script "$REMOTE_REPO_ROOT/open311/etl/aggregate_open311_spark.py"
        """,
        cmd_timeout=None,
    )

    run_baseline_remote = SSHOperator(
        task_id="run_baseline_remote",
        ssh_conn_id=SSH_CONN_ID,
        command="""
        set -euo pipefail
        source "$REMOTE_ENV_FILE"
        cd "$REMOTE_REPO_ROOT"
        bash scripts/open311/run_baseline.sh \
          --bucket "$URBAN_STRESS_BUCKET" \
          --env-file "$REMOTE_ENV_FILE" \
          --spark-submit-bin "$REMOTE_SPARK_SUBMIT_BIN" \
          --lookback-hours "{{ ti.xcom_pull(task_ids='prepare_run_context')['lookback_hours'] }}" \
          --min-history-points "{{ ti.xcom_pull(task_ids='prepare_run_context')['min_history_points'] }}" \
          --spark-script "$REMOTE_REPO_ROOT/open311/baseline/baselines_spark.py"
        """,
        cmd_timeout=None,
    )

    run_context = prepare_run_context()
    ingest = run_ingest_local(run_context)
    bronze = run_bronze_local(run_context)
    silver = run_silver_local(run_context)

    run_context >> ingest >> bronze >> silver >> run_aggregate_remote >> run_baseline_remote