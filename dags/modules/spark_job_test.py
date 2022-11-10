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
    .appName("Census_Data_Collection_Transfrom")  # spark history에서 구분자 사용
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


S3_ACCESS_POINT_ALIAS = (
    "etl-project-bucket-os3osysqodthpasbb5yqre5ot7jeaapn2a-s3alias"
)
S3_DATA_SOURCE_PATH = (
    f"s3a://{S3_ACCESS_POINT_ALIAS}/news_collection/news_collection-s3-sink/*"
)


df = (
    spark.read.format("avro")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "FAILFAST")
    .load(S3_DATA_SOURCE_PATH)
)

df.show()
df.printSchema()


spark.stop()
