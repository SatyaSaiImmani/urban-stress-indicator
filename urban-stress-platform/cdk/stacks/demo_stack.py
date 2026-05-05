"""
demo_stack.py — Single CDK stack for Urban Stress Platform (personal AWS account)
==================================================================================
Resources:
  S3          — one bucket for Glue scripts + bronze data
  RDS         — PostgreSQL 14 db.t3.micro (public, default VPC)
  IAM roles   — pipeline role (Glue/SFN/EB) + ECS task/execution roles
  Glue jobs   — 5 Python Shell jobs (bronze → silver → scoring → composite → gdelt)
  Step Fns    — chains the 5 Glue jobs in sequence
  EventBridge — daily cron at 07:00 UTC
  ECS Fargate — Streamlit dashboard behind an ALB (stable public URL, auto-restart)

Deploy:
  1. cdk bootstrap  (first time only)
  2. cdk deploy     (builds + pushes Docker image automatically, creates everything)
  3. Upload Glue scripts:  bash scripts/build_and_upload_lambdas.sh
  4. Apply schema:  psql -h <RDS endpoint> -U postgres -d postgres -f sql/schema.sql
"""

import json
import os

import aws_cdk as cdk
from aws_cdk import (
    aws_ec2 as ec2,
    aws_ecr_assets as ecr_assets,
    aws_ecs as ecs,
    aws_ecs_patterns as ecs_patterns,
    aws_events as events,
    aws_glue as glue,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_rds as rds,
    aws_s3 as s3,
    aws_stepfunctions as sfn,
)
from constructs import Construct

ACCOUNT = "836734770581"
REGION  = "us-east-1"

# Path to the project root (two levels up from this file)
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "../..")


class DemoStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # ── Default VPC ───────────────────────────────────────────────────────
        vpc = ec2.Vpc.from_lookup(self, "DefaultVpc", is_default=True)

        # ── S3 — single bucket ────────────────────────────────────────────────
        bucket = s3.Bucket(self, "DataBucket",
            bucket_name=f"urban-stress-data-{ACCOUNT}",
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

        # ── RDS security group ────────────────────────────────────────────────
        rds_sg = ec2.SecurityGroup(self, "RdsSg",
            vpc=vpc,
            description="RDS PostgreSQL - open on 5432",
            allow_all_outbound=True,
        )
        rds_sg.add_ingress_rule(
            ec2.Peer.any_ipv4(), ec2.Port.tcp(5432),
            "PostgreSQL - open for Glue (no NAT) and local psql",
        )

        # ── RDS PostgreSQL 14 ─────────────────────────────────────────────────
        db = rds.DatabaseInstance(self, "Db",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_14
            ),
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3, ec2.InstanceSize.MICRO
            ),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            publicly_accessible=True,
            security_groups=[rds_sg],
            database_name="postgres",
            credentials=rds.Credentials.from_generated_secret(
                "postgres",
                secret_name="urban-stress/db-credentials",
            ),
            removal_policy=cdk.RemovalPolicy.DESTROY,
            deletion_protection=False,
            backup_retention=cdk.Duration.days(0),
            multi_az=False,
        )

        # ── IAM role — Glue + Step Functions + EventBridge ───────────────────
        pipeline_role = iam.Role(self, "PipelineRole",
            role_name="urban-stress-pipeline-role",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("glue.amazonaws.com"),
                iam.ServicePrincipal("states.amazonaws.com"),
                iam.ServicePrincipal("events.amazonaws.com"),
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
        )
        bucket.grant_read_write(pipeline_role)
        db.secret.grant_read(pipeline_role)
        pipeline_role.add_to_policy(iam.PolicyStatement(
            actions=["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"],
            resources=["*"],
        ))
        pipeline_role.add_to_policy(iam.PolicyStatement(
            actions=["states:StartExecution"],
            resources=["*"],
        ))

        # ── verdict_gen Lambda ────────────────────────────────────────────────
        # Reads signal_scores for yesterday and writes a verdict to the verdicts table.
        # Code zip is built by scripts/build_and_upload_lambdas.sh and uploaded to S3.
        lambda_role = iam.Role(self, "LambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
            ],
        )
        db.secret.grant_read(lambda_role)

        verdict_fn = lambda_.CfnFunction(self, "VerdictGen",
            function_name="urban-stress-verdict-gen",
            runtime="python3.11",
            handler="handler.handler",
            role=lambda_role.role_arn,
            code=lambda_.CfnFunction.CodeProperty(
                s3_bucket=bucket.bucket_name,
                s3_key="lambdas/verdict_gen.zip",
            ),
            environment=lambda_.CfnFunction.EnvironmentProperty(
                variables={
                    "DB_SECRET_ARN": "urban-stress/db-credentials",
                    "DB_HOST":       db.db_instance_endpoint_address,
                    "DB_PORT":       "5432",
                    "DB_NAME":       "postgres",
                },
            ),
            timeout=120,
        )
        verdict_fn.add_dependency(bucket.node.default_child)

        # Allow Step Functions to invoke the verdict Lambda
        pipeline_role.add_to_policy(iam.PolicyStatement(
            actions=["lambda:InvokeFunction"],
            resources=[f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:urban-stress-verdict-gen"],
        ))

        # ── Glue jobs ─────────────────────────────────────────────────────────
        glue_args = {
            "--DB_SECRET_ARN":              "urban-stress/db-credentials",
            "--BRONZE_BUCKET":              bucket.bucket_name,
            "--SIGNALS_BUCKET":             bucket.bucket_name,
            "--enable-job-bookmarks":       "enable",
            "--additional-python-modules":  "psycopg2-binary",
            "--extra-py-files":             f"s3://{bucket.bucket_name}/glue-scripts/glue_db.py",
        }

        def make_glue_job(job_id: str, script: str) -> glue.CfnJob:
            return glue.CfnJob(self, job_id,
                name=f"urban-stress-{job_id.lower()}",
                role=pipeline_role.role_arn,
                command=glue.CfnJob.JobCommandProperty(
                    name="pythonshell",
                    python_version="3.9",
                    script_location=f"s3://{bucket.bucket_name}/glue-scripts/{script}",
                ),
                default_arguments=glue_args,
                max_capacity=0.0625,
                glue_version="2.0",
                timeout=60,
            )

        make_glue_job("BronzeETL",      "bronze_etl.py")
        make_glue_job("SilverETL",      "silver_etl.py")
        make_glue_job("SignalScore",    "signal_scoring.py")
        make_glue_job("CompositeScore", "composite_score.py")
        make_glue_job("GdeltCollector", "collector_gdelt.py")

        # ── Step Functions state machine ──────────────────────────────────────
        asl = {
            "Comment": "Urban Stress ETL Pipeline",
            "StartAt": "RunBronzeETL",
            "States": {
                "RunBronzeETL":     {"Type": "Task", "Resource": "arn:aws:states:::glue:startJobRun.sync", "Parameters": {"JobName": "urban-stress-bronzeetl"},      "ResultPath": None, "Next": "RunSilverETL"},
                "RunSilverETL":     {"Type": "Task", "Resource": "arn:aws:states:::glue:startJobRun.sync", "Parameters": {"JobName": "urban-stress-silveretl"},      "ResultPath": None, "Next": "RunSignalScoring"},
                "RunSignalScoring": {"Type": "Task", "Resource": "arn:aws:states:::glue:startJobRun.sync", "Parameters": {"JobName": "urban-stress-signalscore"},    "ResultPath": None, "Next": "RunCompositeScore"},
                "RunCompositeScore":{"Type": "Task", "Resource": "arn:aws:states:::glue:startJobRun.sync", "Parameters": {"JobName": "urban-stress-compositescore"}, "ResultPath": None, "Next": "RunGdeltCollector"},
                "RunGdeltCollector": {"Type": "Task", "Resource": "arn:aws:states:::glue:startJobRun.sync",  "Parameters": {"JobName": "urban-stress-gdeltcollector"}, "ResultPath": None, "Next": "RunVerdictGen"},
                "RunVerdictGen":     {"Type": "Task", "Resource": "arn:aws:states:::lambda:invoke",           "Parameters": {"FunctionName": "urban-stress-verdict-gen", "Payload.$": "$"}, "ResultPath": None, "End": True},
            }
        }

        state_machine = sfn.CfnStateMachine(self, "ETLPipeline",
            state_machine_name="urban-stress-etl-pipeline",
            role_arn=pipeline_role.role_arn,
            definition_string=json.dumps(asl),
        )

        # ── EventBridge daily schedule ────────────────────────────────────────
        events.CfnRule(self, "ETLDailySchedule",
            name="urban-stress-etl-daily",
            schedule_expression="cron(0 7 * * ? *)",
            state="ENABLED",
            targets=[events.CfnRule.TargetProperty(
                id="StartETLPipeline",
                arn=f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:urban-stress-etl-pipeline",
                role_arn=pipeline_role.role_arn,
            )],
        )

        # ── ECS Fargate dashboard ─────────────────────────────────────────────

        # Task role — what the running container can do
        ecs_task_role = iam.Role(self, "DashboardTaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        )
        db.secret.grant_read(ecs_task_role)
        bucket.grant_read(ecs_task_role)

        # Execution role — what ECS control plane can do (pull image, write logs)
        ecs_exec_role = iam.Role(self, "DashboardExecRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonECSTaskExecutionRolePolicy"
                ),
            ],
        )
        db.secret.grant_read(ecs_exec_role)  # needed to inject secrets at startup

        # ECS Cluster (Fargate — no EC2 nodes to manage)
        cluster = ecs.Cluster(self, "DashboardCluster",
            cluster_name="urban-stress-dashboard",
            vpc=vpc,
        )

        # Task definition
        task_def = ecs.FargateTaskDefinition(self, "DashboardTaskDef",
            memory_limit_mib=512,
            cpu=256,
            task_role=ecs_task_role,
            execution_role=ecs_exec_role,
        )

        # Build the Docker image from the project root Dockerfile during cdk deploy
        # platform=LINUX_AMD64 is required when building on Apple Silicon (arm64)
        container = task_def.add_container("Dashboard",
            image=ecs.ContainerImage.from_asset(
                PROJECT_ROOT,
                platform=ecr_assets.Platform.LINUX_AMD64,
            ),
            secrets={
                "DB_HOST":     ecs.Secret.from_secrets_manager(db.secret, "host"),
                "DB_USER":     ecs.Secret.from_secrets_manager(db.secret, "username"),
                "DB_PASSWORD": ecs.Secret.from_secrets_manager(db.secret, "password"),
            },
            environment={
                "DB_PORT": "5432",
                "DB_NAME": "postgres",
            },
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="urban-stress",
                log_group=logs.LogGroup(self, "DashboardLogs",
                    log_group_name="/urban-stress/dashboard",
                    retention=logs.RetentionDays.ONE_WEEK,
                    removal_policy=cdk.RemovalPolicy.DESTROY,
                ),
            ),
        )
        container.add_port_mappings(ecs.PortMapping(container_port=8501))

        # ALB + Fargate service — gives a stable public DNS URL
        alb_service = ecs_patterns.ApplicationLoadBalancedFargateService(
            self, "DashboardService",
            cluster=cluster,
            task_definition=task_def,
            public_load_balancer=True,
            listener_port=80,
            desired_count=1,
            assign_public_ip=True,
        )

        # Streamlit health check endpoint
        alb_service.target_group.configure_health_check(
            path="/_stcore/health",
            healthy_http_codes="200",
            interval=cdk.Duration.seconds(30),
            timeout=cdk.Duration.seconds(10),
            healthy_threshold_count=2,
            unhealthy_threshold_count=5,
        )

        # ── Outputs ───────────────────────────────────────────────────────────
        cdk.CfnOutput(self, "DashboardUrl",
            value=f"http://{alb_service.load_balancer.load_balancer_dns_name}",
            description="Streamlit dashboard — stable ALB URL",
        )
        cdk.CfnOutput(self, "DbEndpoint",
            value=db.db_instance_endpoint_address,
            description="RDS endpoint for psql / schema apply",
        )
        cdk.CfnOutput(self, "BucketName",
            value=bucket.bucket_name,
            description="S3 bucket — upload Glue scripts here",
        )
        cdk.CfnOutput(self, "DbSecretArn",
            value=db.secret.secret_arn,
            description="Secrets Manager ARN for DB credentials",
        )
        cdk.CfnOutput(self, "StateMachineArn",
            value=state_machine.attr_arn,
            description="Step Functions ARN for manual trigger",
        )
