#!/usr/bin/env python3
import os

import aws_cdk as cdk

from infrastructure.deployment_stack import DeploymentStack


app = cdk.App()
env = cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT','643553455790'), region=os.getenv('CDK_DEFAULT_REGION','eu-west-1'))

DeploymentStack(
    app,
    "PID",
    env=env
)

app.synth()
