import aws_cdk as cdk
from aws_cdk import (
    aws_s3 as s3,
    aws_lambda as lambda_,
    aws_events as events,
    aws_iam as iam,
)
from constructs import Construct

LAB_ROLE_ARN      = "arn:aws:iam::677542194461:role/LabRole"
ACCOUNT           = "677542194461"
REGION            = "us-east-1"
# Pre-created manually — Lambda zips uploaded here by scripts/build_and_upload_lambdas.sh
ARTIFACTS_BUCKET  = f"urban-stress-artifacts-{ACCOUNT}"


class IngestionStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str,
                 db_endpoint: str, db_secret_arn: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # --- S3 buckets ---
        # auto_delete_objects omitted — creates Lambda+IAM role (blocked by LabRole).
        self.bronze_bucket = s3.Bucket(self, "BronzeBucket",
            bucket_name=f"urban-stress-bronze-{self.account}",
            removal_policy=cdk.RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(expiration=cdk.Duration.days(90))
            ]
        )
        self.raw_bucket = s3.Bucket(self, "RawBucket",
            bucket_name=f"urban-stress-raw-{self.account}",
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )
        self.silver_bucket = s3.Bucket(self, "SilverBucket",
            bucket_name=f"urban-stress-silver-{self.account}",
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )
        self.signals_bucket = s3.Bucket(self, "SignalsBucket",
            bucket_name=f"urban-stress-signals-{self.account}",
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

        # --- Lambda functions (L1 / CfnFunction) ---
        # Using CfnFunction instead of Function to avoid CDK auto-granting
        # CloudWatch Logs permissions (iam:PutRolePolicy — blocked by LabRole).
        # Code is uploaded manually by scripts/build_and_upload_lambdas.sh.
        fn_open311 = lambda_.CfnFunction(self, "CollectorOpen311",
            function_name="urban-stress-collector-open311",
            runtime="python3.11",
            handler="handler.handler",
            code=lambda_.CfnFunction.CodeProperty(
                s3_bucket=ARTIFACTS_BUCKET,
                s3_key="lambdas/collector_open311_v2.zip",
            ),
            role=LAB_ROLE_ARN,
            timeout=300,
            memory_size=256,
            environment=lambda_.CfnFunction.EnvironmentProperty(
                variables={
                    "S3_BRONZE_BUCKET": self.bronze_bucket.bucket_name,
                    "SKIP_DB":          "false",
                    "SKIP_S3":          "false",
                    "DB_HOST":          db_endpoint,
                    "DB_NAME":          "postgres",
                    "DB_PORT":          "5432",
                    "DB_SECRET_ARN":    db_secret_arn,
                }
            ),
        )

        fn_noaa = lambda_.CfnFunction(self, "CollectorNoaa",
            function_name="urban-stress-collector-noaa",
            runtime="python3.11",
            handler="handler.handler",
            code=lambda_.CfnFunction.CodeProperty(
                s3_bucket=ARTIFACTS_BUCKET,
                s3_key="lambdas/collector_noaa_v2.zip",
            ),
            role=LAB_ROLE_ARN,
            timeout=300,
            memory_size=256,
            environment=lambda_.CfnFunction.EnvironmentProperty(
                variables={
                    "S3_BRONZE_BUCKET": self.bronze_bucket.bucket_name,
                    "SKIP_DB":          "false",
                    "SKIP_S3":          "false",
                    "DB_HOST":          db_endpoint,
                    "DB_NAME":          "postgres",
                    "DB_PORT":          "5432",
                    "DB_SECRET_ARN":    db_secret_arn,
                }
            ),
        )

        fn_verdict = lambda_.CfnFunction(self, "VerdictGen",
            function_name="urban-stress-verdict-gen",
            runtime="python3.11",
            handler="handler.handler",
            code=lambda_.CfnFunction.CodeProperty(
                s3_bucket=ARTIFACTS_BUCKET,
                s3_key="lambdas/verdict_gen.zip",
            ),
            role=LAB_ROLE_ARN,
            timeout=60,
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
        self.fn_verdict = fn_verdict

        # --- EventBridge rule: daily at 06:00 UTC (CfnRule — no auto-grants) ---
        open311_arn = (
            f"arn:aws:lambda:{REGION}:{ACCOUNT}"
            f":function:{fn_open311.function_name}"
        )
        noaa_arn = (
            f"arn:aws:lambda:{REGION}:{ACCOUNT}"
            f":function:{fn_noaa.function_name}"
        )

        rule = events.CfnRule(self, "DailyCollectorRule",
            name="urban-stress-daily-collectors",
            schedule_expression="cron(0 6 * * ? *)",
            state="ENABLED",
            targets=[
                events.CfnRule.TargetProperty(id="open311", arn=open311_arn),
                events.CfnRule.TargetProperty(id="noaa",    arn=noaa_arn),
            ]
        )

        # Grant EventBridge permission to invoke each Lambda (Lambda resource policy —
        # does NOT require iam:PutRolePolicy)
        rule_arn = (
            f"arn:aws:events:{REGION}:{ACCOUNT}"
            f":rule/urban-stress-daily-collectors"
        )
        perm_open311 = lambda_.CfnPermission(self, "Open311EventPermission",
            action="lambda:InvokeFunction",
            function_name=fn_open311.function_name,
            principal="events.amazonaws.com",
            source_arn=rule_arn,
        )
        perm_open311.add_dependency(fn_open311)
        perm_open311.add_dependency(rule)

        perm_noaa = lambda_.CfnPermission(self, "NoaaEventPermission",
            action="lambda:InvokeFunction",
            function_name=fn_noaa.function_name,
            principal="events.amazonaws.com",
            source_arn=rule_arn,
        )
        perm_noaa.add_dependency(fn_noaa)
        perm_noaa.add_dependency(rule)

        # Expose for outputs
        self.fn_open311 = fn_open311
        self.fn_noaa    = fn_noaa

        # --- Outputs ---
        cdk.CfnOutput(self, "BronzeBucketName",
            value=self.bronze_bucket.bucket_name,
            export_name="UrbanStress-BronzeBucket"
        )
        cdk.CfnOutput(self, "SignalsBucketName",
            value=self.signals_bucket.bucket_name,
            export_name="UrbanStress-SignalsBucket"
        )
