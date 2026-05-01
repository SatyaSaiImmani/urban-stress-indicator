"""
log_run/handler.py
==================
Triggered by EventBridge on Step Functions execution state changes
(SUCCEEDED, FAILED, TIMED_OUT, ABORTED) for urban-stress-etl-pipeline.

Writes one row to the pipeline_runs table in RDS so the dashboard
can show "last updated" and operators can track pipeline health.

EventBridge event detail shape (Step Functions Execution Status Change):
{
  "executionArn": "arn:aws:states:...:execution:urban-stress-etl-pipeline:...",
  "stateMachineArn": "arn:aws:states:...:stateMachine:urban-stress-etl-pipeline",
  "name": "<execution-name>",
  "status": "SUCCEEDED" | "FAILED" | "TIMED_OUT" | "ABORTED",
  "startDate": <epoch-ms>,
  "stopDate":  <epoch-ms>,
  "input":  "...",
  "cause":  "...",   # present on failure
  "error":  "..."    # present on failure
}
"""

from datetime import datetime, timezone

from db import get_connection


def handler(event, context=None):
    detail = event.get("detail", {})

    execution_arn = detail.get("executionArn", "")
    sm_arn        = detail.get("stateMachineArn", "")
    pipeline_name = sm_arn.split(":")[-1] if sm_arn else "unknown"
    status        = detail.get("status", "UNKNOWN")
    start_ms      = detail.get("startDate")
    stop_ms       = detail.get("stopDate")

    duration_secs = None
    if start_ms and stop_ms:
        duration_secs = max(0, int((stop_ms - start_ms) / 1000))

    run_at = (
        datetime.fromtimestamp(stop_ms / 1000, tz=timezone.utc).isoformat()
        if stop_ms
        else datetime.now(tz=timezone.utc).isoformat()
    )

    error_message = None
    if status in ("FAILED", "TIMED_OUT", "ABORTED"):
        error  = detail.get("error", "")
        cause  = detail.get("cause", "")
        error_message = f"{error}: {cause}".strip(": ") if error or cause else status

    print(f"Logging pipeline run: {pipeline_name} → {status} ({duration_secs}s)")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_runs
                  (pipeline_name, execution_arn, run_at, status, duration_secs, error_message)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (pipeline_name, execution_arn, run_at, status, duration_secs, error_message))
        conn.commit()
    finally:
        conn.close()

    return {"status": "ok", "pipeline": pipeline_name, "result": status}
