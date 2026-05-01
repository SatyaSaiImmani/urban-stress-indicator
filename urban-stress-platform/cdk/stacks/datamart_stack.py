import aws_cdk as cdk
from aws_cdk import (
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_secretsmanager as sm,
)
from constructs import Construct

LAB_ROLE_ARN = "arn:aws:iam::677542194461:role/LabRole"


class DatamartStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # --- VPC: public subnets only, no NAT Gateway ---
        # Lambdas run outside the VPC so they get free internet access via AWS managed NAT.
        # RDS is publicly accessible (security group restricts access).
        # ECS Fargate uses assign_public_ip=True through the Internet Gateway.
        self.vpc = ec2.Vpc(self, "UrbanStressVpc",
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24
                ),
            ]
        )

        # --- Security group for RDS ---
        self.rds_sg = ec2.SecurityGroup(self, "RdsSg",
            vpc=self.vpc,
            description="Allow Postgres from anywhere (class project - lock down in prod)",
            allow_all_outbound=True
        )
        self.rds_sg.add_ingress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.tcp(5432),
            "Postgres from anywhere"
        )

        # --- RDS credentials in Secrets Manager ---
        self.db_secret = sm.Secret(self, "DbSecret",
            secret_name="urban-stress/db-credentials",
            generate_secret_string=sm.SecretStringGenerator(
                secret_string_template='{"username": "postgres"}',
                generate_string_key="password",
                exclude_punctuation=True,
                include_space=False,
            )
        )

        # --- RDS PostgreSQL (public subnet, publicly accessible) ---
        # publicly_accessible=True required because there is no private subnet / NAT.
        # Monitoring interval 0 avoids auto-creating an enhanced monitoring IAM role
        # (iam:CreateRole is blocked in Learner Lab).
        self.db = rds.DatabaseInstance(self, "UrbanStressDb",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_15
            ),
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3,
                ec2.InstanceSize.MICRO
            ),
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PUBLIC
            ),
            security_groups=[self.rds_sg],
            credentials=rds.Credentials.from_secret(self.db_secret),
            database_name="postgres",
            allocated_storage=20,
            publicly_accessible=True,
            deletion_protection=False,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            monitoring_interval=cdk.Duration.seconds(0),
        )

        # Expose endpoint for other stacks
        self.db_endpoint = self.db.db_instance_endpoint_address

        # --- Outputs ---
        cdk.CfnOutput(self, "VpcId",
            value=self.vpc.vpc_id,
            export_name="UrbanStress-VpcId"
        )
        cdk.CfnOutput(self, "DbSecretArn",
            value=self.db_secret.secret_arn,
            export_name="UrbanStress-DbSecretArn"
        )
        cdk.CfnOutput(self, "DbEndpoint",
            value=self.db_endpoint,
            export_name="UrbanStress-DbEndpoint"
        )
