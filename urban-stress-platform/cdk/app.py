import aws_cdk as cdk
from stacks.demo_stack import DemoStack

app = cdk.App()

DemoStack(app, "UrbanStressDemo",
    env=cdk.Environment(account="836734770581", region="us-east-1"),
    description="Urban Stress Intelligence Platform — demo stack",
)

app.synth()
