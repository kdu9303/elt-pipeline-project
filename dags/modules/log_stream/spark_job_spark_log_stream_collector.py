# -*- coding: utf-8 -*-
import os
import configparser
from typing import Dict
from pyspark.sql import SparkSession


def get_aws_keys(profile: str) -> Dict[str, str]:
    awsCreds = {}
    Config = configparser.ConfigParser()
    Config.read(os.path.expanduser("/home/ec2-user/.aws/credentials"))
    if profile in Config.sections():
        awsCreds["aws_access_key_id"] = Config.get(
            profile, "aws_access_key_id"
        )
        awsCreds["aws_secret_access_key"] = Config.get(
            profile, "aws_secret_access_key"
        )
    return awsCreds


awsKeys = get_aws_keys("default")
Access_key_ID = awsKeys["aws_access_key_id"]
Secret_access_key = awsKeys["aws_secret_access_key"]
# -------------------------------------------------------------------
# spark config
# -------------------------------------------------------------------
spark = (
    SparkSession.builder.master("yarn")
    .config("spark.submit.deployMode", "cluster")
    .appName("Spark_Log_Stream_Transfrom")  # spark history에서 구분자 사용
    .config("spark.driver.memory", "2g")
    .config("spark.driver.cores", 1)
    .config(
        "spark.driver.extraJavaOptions",
        "-Dlog4j.configuration=log4j.properties",
    )
    .config("spark.executor.instances", 2)
    .config("spark.executor.memory", "2g")
    .config(
        "spark.executor.extraJavaOptions",
        "-Dlog4j.configuration=log4j.properties",
    )
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12-3.2.2",
    )  # spark streaming
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    .getOrCreate()
)

# delta lake package path
spark.sparkContext.addPyFile(
    "/home/ec2-user/spark/jars/delta-core_2.12-2.0.0.jar"
)

# log4j property file
spark.sparkContext.addFile("/home/ec2-user/spark/conf/log4j.properties")

# spark session을 생성한 이후 delta lake 모듈을 불러올 수 있음
from delta.tables import *  # noqa: E402, F401, F403, F405

# -------------------------------------------------------------------
# hadoop s3a config
# -------------------------------------------------------------------
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
)
hadoop_conf.set(
    "fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A"
)
hadoop_conf.set("fs.s3a.access.key", Access_key_ID)
hadoop_conf.set("fs.s3a.secret.key", Secret_access_key)
hadoop_conf.set("fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
hadoop_conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.4")
hadoop_conf.set(
    "spark.delta.logStore.class",
    "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
)

# -------------------------------------------------------------------
# Data setting
# -------------------------------------------------------------------

table_name = "spark_application_log"

# delta target path 설정
S3_DELTA_ACCESS_POINT_ALIAS = (
    "etl-project-bucket-d-5j9ze83mfdo93wc49uth4j369r9o4apn2b-s3alias"
)
S3_DATA_DELTA_PATH = f"s3a://{S3_DELTA_ACCESS_POINT_ALIAS}/{table_name}/"

s3_CHECKPOINT_PATH = "/home/ec2-user/spark-data/stream_checkpoints"


KAFKA_BOOTSTRAP_SERVERS = (
    "43.201.13.181:9092,43.200.251.62:9092,52.78.78.140:9092"
)
KAFKA_TOPIC = "spark-application-log"


stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)


stream_df.selectExpr(
    "timestamp", "CAST(key AS STRING)", "CAST(value AS STRING)"
).writeStream.format("delta").outputMode("append").option(
    "checkpointLocation", s3_CHECKPOINT_PATH
).start(
    S3_DATA_DELTA_PATH
)
