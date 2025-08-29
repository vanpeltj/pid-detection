import os
from aws_cdk import (
    aws_lambda,
    Duration,
    aws_sqs,
    aws_lambda_event_sources, aws_iam,
)
from constructs import Construct

class PidProcessingFrameworkConstruct(Construct):
    def __init__(self, scope: Construct, construct_id: str, stack_name, bucket,db_secret, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.stack_name = stack_name
        self.db_secret = db_secret
        self.bucket = bucket

        process_pid_pdf_lambda = aws_lambda.DockerImageFunction(
            self,
            "ProcessPIDPdf",
            function_name=f"{self.stack_name}-process-pid-pdf",
            code=aws_lambda.DockerImageCode.from_image_asset(
                directory="./assets",
                file = "lambda/process_pid_pdf/Dockerfile",
                build_args = {
                    "FUNCTION_CODE":"lambda/process_pid_pdf"
                }
            ),
            memory_size=512,
            timeout=Duration.minutes(2),
            environment={
                "S3_BUCKET": self.bucket.bucket_name,
            }
        )
        self.bucket.grant_read_write(process_pid_pdf_lambda)
        self.db_secret.grant_read(process_pid_pdf_lambda)
        process_pid_pdf_lambda.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["ssm:GetParameter"],
                resources=["*"],
            )
        )


        self.pid_file_processing_queue = aws_sqs.Queue(
            self, "MyQueue",
            visibility_timeout=Duration.seconds(121),  # how long a msg is hidden after being picked up
        )

        process_pid_pdf_lambda.add_event_source(
            aws_lambda_event_sources.SqsEventSource(self.pid_file_processing_queue, batch_size=2)
        )