-- Migration 001: Replace pipeline_runs with Step Functions-aware schema
-- Run once against RDS: psql $DB_URL -f sql/migrations/001_pipeline_runs_v2.sql

-- Drop and recreate — original table was never populated so no data loss.
DROP TABLE IF EXISTS pipeline_runs;

CREATE TABLE pipeline_runs (
  id              SERIAL PRIMARY KEY,
  pipeline_name   TEXT        NOT NULL,          -- 'urban-stress-etl-pipeline'
  execution_arn   TEXT,                          -- Step Functions execution ARN
  run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  status          TEXT        NOT NULL,          -- 'SUCCEEDED' | 'FAILED' | 'TIMED_OUT' | 'ABORTED'
  duration_secs   INT,                           -- stopDate - startDate in seconds
  error_message   TEXT                           -- populated on failure
);

CREATE INDEX idx_pipeline_runs_status  ON pipeline_runs (status, run_at DESC);
CREATE INDEX idx_pipeline_runs_name    ON pipeline_runs (pipeline_name, run_at DESC);
