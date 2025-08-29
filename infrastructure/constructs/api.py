from aws_cdk import (
    aws_ec2,
    aws_iam,
    aws_rds,
    aws_sqs,
    aws_s3,
    aws_ssm,
    RemovalPolicy,
    aws_lambda,
    Duration,
    aws_s3,aws_apigateway
)
from constructs import Construct

class APIConstruct(Construct):
    def __init__(
            self,
            scope: Construct,
            construct_id: str,
            stack_name,
            bucket: aws_s3.Bucket,
            db_secret,
            db_instance:  aws_rds.DatabaseInstance,
            pid_processing_queue: aws_sqs.Queue,
            **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.stack_name = stack_name
        self.bucket = bucket
        self.db_secret = db_secret
        self.db_instance = db_instance

        self.pid_processing_queue = pid_processing_queue




        api_lambda = aws_lambda.DockerImageFunction(
            self,
            "APILambda",
            function_name=f"{self.stack_name}-pid-api",
            code=aws_lambda.DockerImageCode.from_image_asset(
                directory="./assets",
                file = "lambda/api/Dockerfile",
                build_args = {
                    "FUNCTION_CODE":"lambda/api"
                }
            ),
            memory_size=512,
            timeout=Duration.minutes(2),
            environment={
                "S3_BUCKET": self.bucket.bucket_name,
                "DB_SECRET_ARN": self.db_secret.secret_arn,
                "DB_NAME": "pid",
                "PID_PROCESSING_QUEUE_URL": self.pid_processing_queue.queue_url,
            }
        )
        self.db_secret.grant_read(api_lambda)
        self.bucket.grant_read_write(api_lambda)
        self.pid_processing_queue.grant_send_messages(api_lambda)

        api_lambda.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["ssm:GetParameter"],
                resources=["*"],
            )
        )

        # Create API Gateway (regional instead of edge optimized)
        api = aws_apigateway.RestApi(
            self,
            "FastApi",
            rest_api_name="pid-api",
            description="FastAPI running on Lambda",
            endpoint_configuration=aws_apigateway.EndpointConfiguration(
                types=[aws_apigateway.EndpointType.REGIONAL]
            ),
            binary_media_types=[
                "application/pdf",
                "multipart/form-data",
                "application/octet-stream"
            ],  # Allow all binary media types

        )


        # Integration: Lambda → API Gateway
        integration = aws_apigateway.LambdaIntegration(api_lambda)
        api.root.add_method("ANY", integration)  # root route
        api.root.add_resource("{proxy+}").add_method("ANY", integration)



        api_url =aws_ssm.StringParameter(
            self,
            "ApiUrlParameter",
            parameter_name=f"/micro-services/api/url",
            string_value=api.url[:-1]
        )

