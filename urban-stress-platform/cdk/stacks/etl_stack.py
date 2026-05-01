import json
import aws_cdk as cdk
from aws_cdk import (
    aws_glue as glue,
    aws_stepfunctions as sfn,
    aws_s3 as s3,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_events as events,
    aws_cloudwatch as cw,
)
from constructs import Construct

LAB_ROLE_ARN     = "arn:aws:iam::677542194461:role/LabRole"
ACCOUNT          = "677542194461"
REGION           = "us-east-1"
ARTIFACTS_BUCKET = f"urban-stress-artifacts-{ACCOUNT}"


class ETLStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str,
                 bronze_bucket: s3.Bucket,
                 signals_bucket: s3.Bucket,
                 db_endpoint: str,
                 db_secret_arn: str,
                 **kwargs):
        super().__init__(scope, id, **kwargs)

        # ── Glue jobs ─────────────────────────────────────────────────────────
        glue_args = {
            "--DB_SECRET_ARN":              "urban-stress/db-credentials",
            "--BRONZE_BUCKET":              bronze_bucket.bucket_name,
            "--SIGNALS_BUCKET":             signals_bucket.bucket_name,
            "--enable-job-bookmarks":       "enable",
            "--additional-python-modules":  "psycopg2-binary",
            "--extra-py-files":             f"s3://{bronze_bucket.bucket_name}/glue-scripts/glue_db.py",
        }

        def make_glue_job(job_id: str, script_path: str) -> glue.CfnJob:
            return glue.CfnJob(self, job_id,
                name=f"urban-stress-{job_id.lower()}",
                role=LAB_ROLE_ARN,
                command=glue.CfnJob.JobCommandProperty(
                    name="pythonshell",
                    python_version="3.9",
                    script_location=(
                        f"s3://{bronze_bucket.bucket_name}/glue-scripts/{script_path}"
                    )
                ),
                default_arguments=glue_args,
                max_capacity=0.0625,
                glue_version="2.0",
                timeout=60,
            )

        self.job_bronze    = make_glue_job("BronzeETL",      "bronze_etl.py")
        self.job_silver    = make_glue_job("SilverETL",      "silver_etl.py")
        self.job_scoring   = make_glue_job("SignalScore",    "signal_scoring.py")
        self.job_composite = make_glue_job("CompositeScore", "composite_score.py")
        self.job_gdelt     = make_glue_job("GdeltCollector", "collector_gdelt.py")

        # ── Step Functions ────────────────────────────────────────────────────
        asl = {
            "Comment": "Urban Stress ETL Pipeline",
            "StartAt": "RunBronzeETL",
            "States": {
                "RunBronzeETL": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {"JobName": "urban-stress-bronzeetl"},
                    "ResultPath": None,
                    "Next": "RunSilverETL"
                },
                "RunSilverETL": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {"JobName": "urban-stress-silveretl"},
                    "ResultPath": None,
                    "Next": "RunSignalScoring"
                },
                "RunSignalScoring": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {"JobName": "urban-stress-signalscore"},
                    "ResultPath": None,
                    "Next": "RunCompositeScore"
                },
                "RunCompositeScore": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {"JobName": "urban-stress-compositescore"},
                    "ResultPath": None,
                    "Next": "RunGdeltCollector"
                },
                "RunGdeltCollector": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {"JobName": "urban-stress-gdeltcollector"},
                    "ResultPath": None,
                    "End": True
                }
            }
        }

        self.state_machine = sfn.CfnStateMachine(self, "ETLPipeline",
            state_machine_name="urban-stress-etl-pipeline",
            role_arn=LAB_ROLE_ARN,
            definition_string=json.dumps(asl),
        )

        state_machine_arn = (
            f"arn:aws:states:{REGION}:{ACCOUNT}"
            f":stateMachine:urban-stress-etl-pipeline"
        )

        # ── log_run Lambda ────────────────────────────────────────────────────
        # Writes one row to pipeline_runs on every SFN execution completion.
        fn_log_run = lambda_.CfnFunction(self, "LogRun",
            function_name="urban-stress-log-run",
            runtime="python3.11",
            handler="handler.handler",
            code=lambda_.CfnFunction.CodeProperty(
                s3_bucket=ARTIFACTS_BUCKET,
                s3_key="lambdas/log_run.zip",
            ),
            role=LAB_ROLE_ARN,
            timeout=30,
            memory_size=128,
            environment=lambda_.CfnFunction.EnvironmentProperty(
                variables={
                    "DB_HOST":       db_endpoint,
                    "DB_NAME":       "postgres",
                    "DB_PORT":       "5432",
                    "DB_SECRET_ARN": db_secret_arn,
                }
            ),
        )

        log_run_arn = (
            f"arn:aws:lambda:{REGION}:{ACCOUNT}"
            f":function:urban-stress-log-run"
        )

        # EventBridge rule: SFN execution status change → log_run Lambda
        state_change_rule = events.CfnRule(self, "PipelineStateChangeRule",
            name="urban-stress-pipeline-state-change",
            state="ENABLED",
            event_pattern={
                "source": ["aws.states"],
                "detail-type": ["Step Functions Execution Status Change"],
                "detail": {
                    "stateMachineArn": [state_machine_arn],
                    "status": ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"],
                },
            },
            targets=[
                events.CfnRule.TargetProperty(
                    id="LogPipelineRun",
                    arn=log_run_arn,
                )
            ],
        )
        state_change_rule.add_dependency(fn_log_run)
        state_change_rule.add_dependency(self.state_machine)

        # Lambda resource policy: allow EventBridge to invoke log_run
        perm_log_run = lambda_.CfnPermission(self, "LogRunEventPermission",
            action="lambda:InvokeFunction",
            function_name=fn_log_run.function_name,
            principal="events.amazonaws.com",
            source_arn=(
                f"arn:aws:events:{REGION}:{ACCOUNT}"
                f":rule/urban-stress-pipeline-state-change"
            ),
        )
        perm_log_run.add_dependency(fn_log_run)
        perm_log_run.add_dependency(state_change_rule)

        # ── CloudWatch alarms ─────────────────────────────────────────────────
        # One alarm on ExecutionsFailed covers all 5 Glue jobs — any Glue
        # failure propagates up to a SFN execution failure.
        cw.CfnAlarm(self, "ETLFailedAlarm",
            alarm_name="urban-stress-etl-pipeline-failed",
            alarm_description=(
                "One or more urban-stress-etl-pipeline executions failed. "
                "Check Step Functions console for the failing Glue job."
            ),
            namespace="AWS/States",
            metric_name="ExecutionsFailed",
            dimensions=[
                cw.CfnAlarm.DimensionProperty(
                    name="StateMachineArn",
                    value=state_machine_arn,
                )
            ],
            statistic="Sum",
            period=300,
            evaluation_periods=1,
            threshold=1,
            comparison_operator="GreaterThanOrEqualToThreshold",
            treat_missing_data="notBreaching",
        )

        cw.CfnAlarm(self, "ETLTimedOutAlarm",
            alarm_name="urban-stress-etl-pipeline-timed-out",
            alarm_description="urban-stress-etl-pipeline execution timed out.",
            namespace="AWS/States",
            metric_name="ExecutionsTimedOut",
            dimensions=[
                cw.CfnAlarm.DimensionProperty(
                    name="StateMachineArn",
                    value=state_machine_arn,
                )
            ],
            statistic="Sum",
            period=300,
            evaluation_periods=1,
            threshold=1,
            comparison_operator="GreaterThanOrEqualToThreshold",
            treat_missing_data="notBreaching",
        )

        # ── EventBridge daily schedule ────────────────────────────────────────
        # Collectors fire at 06:00 UTC; ETL starts at 07:00 UTC (60-min buffer).
        schedule_rule = events.CfnRule(self, "ETLDailySchedule",
            name="urban-stress-etl-daily",
            schedule_expression="cron(0 7 * * ? *)",
            state="ENABLED",
            targets=[
                events.CfnRule.TargetProperty(
                    id="StartETLPipeline",
                    arn=state_machine_arn,
                    role_arn=LAB_ROLE_ARN,
                )
            ],
        )
        schedule_rule.add_dependency(self.state_machine)

        # ── Outputs ───────────────────────────────────────────────────────────
        cdk.CfnOutput(self, "StateMachineArn",
            value=self.state_machine.attr_arn,
            export_name="UrbanStress-StateMachineArn"
        )
