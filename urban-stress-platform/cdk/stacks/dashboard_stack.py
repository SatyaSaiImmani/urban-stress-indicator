import aws_cdk as cdk
from aws_cdk import (
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_elasticloadbalancingv2 as elbv2,
    aws_logs as logs,
    aws_iam as iam,
)
from constructs import Construct

LAB_ROLE_ARN = "arn:aws:iam::677542194461:role/LabRole"
ACCOUNT      = "677542194461"
REGION       = "us-east-1"


class DashboardStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str,
                 vpc: ec2.Vpc,
                 db_endpoint: str,
                 db_secret_arn: str,
                 **kwargs):
        super().__init__(scope, id, **kwargs)

        # --- CloudWatch log group for ECS task logs ---
        log_group = logs.CfnLogGroup(self, "DashboardLogGroup",
            log_group_name="/ecs/urban-stress-dashboard",
            retention_in_days=30,
        )

        # --- Security groups ---
        # ALB: accept HTTP from anywhere
        alb_sg = ec2.CfnSecurityGroup(self, "AlbSg",
            group_description="ALB - allow HTTP from anywhere",
            vpc_id=vpc.vpc_id,
            security_group_ingress=[
                ec2.CfnSecurityGroup.IngressProperty(
                    ip_protocol="tcp", from_port=80, to_port=80,
                    cidr_ip="0.0.0.0/0"
                )
            ],
        )

        # ECS task: accept 8501 from ALB SG only
        task_sg = ec2.CfnSecurityGroup(self, "TaskSg",
            group_description="ECS task - allow 8501 from ALB",
            vpc_id=vpc.vpc_id,
            security_group_ingress=[
                ec2.CfnSecurityGroup.IngressProperty(
                    ip_protocol="tcp", from_port=8501, to_port=8501,
                    source_security_group_id=alb_sg.attr_group_id
                )
            ],
        )

        # --- ALB ---
        public_subnet_ids = vpc.select_subnets(
            subnet_type=ec2.SubnetType.PUBLIC
        ).subnet_ids

        alb = elbv2.CfnLoadBalancer(self, "DashboardAlb",
            name="urban-stress-alb",
            scheme="internet-facing",
            type="application",
            subnets=public_subnet_ids,
            security_groups=[alb_sg.attr_group_id],
        )

        # --- Target group (IP mode for Fargate) ---
        tg = elbv2.CfnTargetGroup(self, "DashboardTg",
            name="urban-stress-tg",
            port=8501,
            protocol="HTTP",
            target_type="ip",
            vpc_id=vpc.vpc_id,
            health_check_path="/_stcore/health",
            health_check_protocol="HTTP",
            health_check_interval_seconds=30,
            healthy_threshold_count=2,
            unhealthy_threshold_count=3,
            matcher=elbv2.CfnTargetGroup.MatcherProperty(http_code="200"),
        )

        # --- Listener: HTTP:80 → target group ---
        listener = elbv2.CfnListener(self, "DashboardListener",
            load_balancer_arn=alb.ref,
            port=80,
            protocol="HTTP",
            default_actions=[
                elbv2.CfnListener.ActionProperty(
                    type="forward",
                    target_group_arn=tg.ref,
                )
            ],
        )

        # --- ECS Cluster ---
        cluster = ecs.CfnCluster(self, "DashboardCluster",
            cluster_name="urban-stress-cluster",
        )

        # --- Task definition ---
        ecr_image = (
            f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com"
            f"/urban-stress-dashboard:latest"
        )
        task_def = ecs.CfnTaskDefinition(self, "DashboardTaskDef",
            family="urban-stress-dashboard",
            cpu="256",
            memory="512",
            network_mode="awsvpc",
            requires_compatibilities=["FARGATE"],
            task_role_arn=LAB_ROLE_ARN,
            execution_role_arn=LAB_ROLE_ARN,
            container_definitions=[
                ecs.CfnTaskDefinition.ContainerDefinitionProperty(
                    name="dashboard",
                    image=ecr_image,
                    essential=True,
                    port_mappings=[
                        ecs.CfnTaskDefinition.PortMappingProperty(
                            container_port=8501,
                            protocol="tcp"
                        )
                    ],
                    environment=[
                        ecs.CfnTaskDefinition.KeyValuePairProperty(
                            name="DB_HOST",       value=db_endpoint),
                        ecs.CfnTaskDefinition.KeyValuePairProperty(
                            name="DB_PORT",       value="5432"),
                        ecs.CfnTaskDefinition.KeyValuePairProperty(
                            name="DB_NAME",       value="postgres"),
                        ecs.CfnTaskDefinition.KeyValuePairProperty(
                            name="DB_SECRET_ARN", value=db_secret_arn),
                        ecs.CfnTaskDefinition.KeyValuePairProperty(
                            name="DB_USER",       value="postgres"),
                    ],
                    # Inject DB_PASSWORD directly from Secrets Manager —
                    # ECS fetches the value at task start, no Docker rebuild needed.
                    secrets=[
                        ecs.CfnTaskDefinition.SecretProperty(
                            name="DB_PASSWORD",
                            value_from=f"{db_secret_arn}:password::",
                        )
                    ],
                    log_configuration=ecs.CfnTaskDefinition.LogConfigurationProperty(
                        log_driver="awslogs",
                        options={
                            "awslogs-group":         "/ecs/urban-stress-dashboard",
                            "awslogs-region":        REGION,
                            "awslogs-stream-prefix": "ecs",
                        }
                    ),
                )
            ],
        )
        task_def.add_dependency(log_group)

        # --- ECS Fargate Service ---
        # assign_public_ip=ENABLED + public subnet → Internet Gateway, no NAT needed
        service = ecs.CfnService(self, "DashboardService",
            cluster=cluster.ref,
            service_name="urban-stress-dashboard",
            task_definition=task_def.ref,
            desired_count=1,
            launch_type="FARGATE",
            network_configuration=ecs.CfnService.NetworkConfigurationProperty(
                awsvpc_configuration=ecs.CfnService.AwsVpcConfigurationProperty(
                    subnets=public_subnet_ids,
                    security_groups=[task_sg.attr_group_id],
                    assign_public_ip="ENABLED",
                )
            ),
            load_balancers=[
                ecs.CfnService.LoadBalancerProperty(
                    container_name="dashboard",
                    container_port=8501,
                    target_group_arn=tg.ref,
                )
            ],
            deployment_configuration=ecs.CfnService.DeploymentConfigurationProperty(
                minimum_healthy_percent=0,
                maximum_percent=200,
            ),
        )
        service.add_dependency(task_def)
        # Listener must be created first — it's what associates the target group
        # with the ALB. Without this, ECS service creation fails with
        # "target group does not have an associated load balancer".
        service.add_dependency(listener)

        # --- Outputs ---
        cdk.CfnOutput(self, "DashboardUrl",
            value=cdk.Fn.sub("http://${AlbDns}", {"AlbDns": alb.attr_dns_name}),
            export_name="UrbanStress-DashboardUrl"
        )
        cdk.CfnOutput(self, "EcrRepoUri",
            value=f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com/urban-stress-dashboard",
            export_name="UrbanStress-EcrRepoUri"
        )
