import aws_cdk as cdk
from stacks.datamart_stack import DatamartStack
from stacks.ingestion_stack import IngestionStack
from stacks.etl_stack import ETLStack
from stacks.dashboard_stack import DashboardStack

ENV = cdk.Environment(account="677542194461", region="us-east-1")

# DefaultStackSynthesizer with generate_bootstrap_version_rule=False:
# skips the SSM /cdk-bootstrap/hnb659fds/version check that requires
# `cdk bootstrap`, which is blocked in AWS Academy Learner Lab.
def synth(stack_id: str) -> cdk.DefaultStackSynthesizer:
    return cdk.DefaultStackSynthesizer(
        generate_bootstrap_version_rule=False,
        deploy_role_arn="arn:aws:iam::677542194461:role/LabRole",
        file_asset_publishing_role_arn="arn:aws:iam::677542194461:role/LabRole",
        image_asset_publishing_role_arn="arn:aws:iam::677542194461:role/LabRole",
        cloud_formation_execution_role="arn:aws:iam::677542194461:role/LabRole",
        lookup_role_arn="arn:aws:iam::677542194461:role/LabRole",
    )

app = cdk.App()

datamart = DatamartStack(app, "UrbanStressDatamart",
    synthesizer=synth("datamart"), env=ENV)

ingestion = IngestionStack(app, "UrbanStressIngestion",
    db_endpoint=datamart.db_endpoint,
    db_secret_arn=datamart.db_secret.secret_arn,
    synthesizer=synth("ingestion"), env=ENV)

etl = ETLStack(app, "UrbanStressETL",
    bronze_bucket=ingestion.bronze_bucket,
    signals_bucket=ingestion.signals_bucket,
    db_endpoint=datamart.db_endpoint,
    db_secret_arn=datamart.db_secret.secret_arn,
    synthesizer=synth("etl"), env=ENV)

dashboard = DashboardStack(app, "UrbanStressDashboard",
    vpc=datamart.vpc,
    db_endpoint=datamart.db_endpoint,
    db_secret_arn=datamart.db_secret.secret_arn,
    synthesizer=synth("dashboard"), env=ENV)

app.synth()
