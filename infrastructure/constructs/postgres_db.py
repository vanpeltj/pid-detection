from aws_cdk import aws_ec2, aws_rds, RemovalPolicy, aws_ssm
from constructs import Construct

class DatabaseConstruct(Construct):
    def __init__(self, scope: Construct, construct_id: str, stack_name, vpc: aws_ec2.Vpc, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.vpc = vpc

        # Security group for DB
        sg = aws_ec2.SecurityGroup(self, "PostgresSG", vpc=vpc, allow_all_outbound=True)
        sg.add_ingress_rule(aws_ec2.Peer.any_ipv4(), aws_ec2.Port.tcp(5432), "Allow public Postgres access")



        db = aws_rds.DatabaseInstance(
            self,
            "PostgresInstance",
            engine=aws_rds.DatabaseInstanceEngine.postgres(version=aws_rds.PostgresEngineVersion.VER_15),
            vpc=vpc,
            vpc_subnets={"subnet_type": aws_ec2.SubnetType.PUBLIC},  # use public subnets in default VPC
            instance_type=aws_ec2.InstanceType.of(
                aws_ec2.InstanceClass.BURSTABLE3, aws_ec2.InstanceSize.MICRO
            ),
            security_groups=[sg],
            allocated_storage=20,
            removal_policy=RemovalPolicy.DESTROY,
            deletion_protection=False,
            publicly_accessible=True,  # ✅ make it accessible from the internet
            credentials=aws_rds.Credentials.from_generated_secret("postgres"),
        )
        self.db_secret = db.secret
        self.db_instance = db



