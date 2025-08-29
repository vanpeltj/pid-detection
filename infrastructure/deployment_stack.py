from aws_cdk import (
    Duration,
    Stack,
    aws_s3,
    RemovalPolicy,
    aws_ec2,
    aws_rds
)
from constructs import Construct

from .constructs.api import APIConstruct
from .constructs.pid_processing_framework import PidProcessingFrameworkConstruct
from .constructs.postgres_db import DatabaseConstruct


class DeploymentStack(Stack):

    def __init__(self, scope: Construct, stack_name: str, **kwargs) -> None:
        super().__init__(scope, stack_name, **kwargs)

        bucket = aws_s3.Bucket(
            self,
            "DataBucket",
            bucket_name=f"{self.account}-{self.region}-files",
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            encryption=aws_s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN
        )

        vpc = aws_ec2.Vpc.from_lookup(self, "DefaultVPC", is_default=True)

        db_construct = DatabaseConstruct(
            scope=self,
            construct_id="DBConstruct",
            vpc=vpc,
            stack_name = stack_name
        )

        pid_processing_framework_construct = PidProcessingFrameworkConstruct(
            scope=self,
            construct_id = "PIDProcessingFrameworkConstruct",
            bucket=bucket,
            stack_name=stack_name,
            db_secret=db_construct.db_secret,
        )

        APIConstruct(
            scope=self,
            construct_id="ApiConstruct",
            bucket=bucket,
            stack_name=stack_name,
            db_secret = db_construct.db_secret,
            db_instance=db_construct.db_instance,
            pid_processing_queue=pid_processing_framework_construct.pid_file_processing_queue
        )
