# -*- coding: utf-8 -*-
from diagrams import Cluster, Diagram, Edge
from diagrams.onprem.workflow import Airflow
from diagrams.aws.storage import S3
from diagrams.aws.analytics import Glue
from diagrams.aws.database import RDS
from diagrams.onprem.queue import Kafka

with Diagram("ELT Pipeline", show=True):

    producer = Kafka("Producers")
    scheduler = Airflow("Scheduler on EC2 Docker")
    scheduler << Edge(color="blue", style="dashed") << producer

    with Cluster("Kafka Cluster"):
        broker = [Kafka("broker1"), Kafka("broker2"), Kafka("broker3")]

    sink_connector = Kafka("S3 sink connector")
    datalake = S3("S3 Datalake")
    transform = Glue("Spark job")
    dw = RDS("MYSQL")

    producer >> broker >> sink_connector >> datalake >> transform >> dw
