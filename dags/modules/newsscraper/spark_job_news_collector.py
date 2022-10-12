# -*- coding: utf-8 -*-
import os
import configparser
from datetime import datetime
from typing import Dict
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException


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

# spark config
spark = (
    SparkSession.builder.master("yarn")
    .appName("test")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "2g")
    .config("spark.yarn.am.memory", "2g")
    .config("spark.executor.cores", 3)
    .config("spark.executor.instances", 1)
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

# delta lake package path
spark.sparkContext.addPyFile(
    "/home/ec2-user/spark/jars/delta-core_2.12-2.0.0.jar"
)

# spark session을 생성한 이후 delta lake 모듈을 불러올 수 있음
from delta.tables import *  # noqa: E402, F403, F405

# hadoop s3a config
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

# source path 설정
S3_SOURCE_ACCESS_POINT_ALIAS = (
    "etl-project-bucket-os3osysqodthpasbb5yqre5ot7jeaapn2a-s3alias"
)
SORUCE_PROCESS_DATE = datetime.now().strftime("%Y-%m-%d")
S3_DATA_SOURCE_PATH = f"s3a://{S3_SOURCE_ACCESS_POINT_ALIAS}/news_collection/news_collection-s3-sink/{SORUCE_PROCESS_DATE}/*"

source_df = (
    spark.read.format("avro")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "FAILFAST")
    .load(S3_DATA_SOURCE_PATH)
)

# delta target path 설정
S3_DELTA_ACCESS_POINT_ALIAS = (
    "etl-project-bucket-d-5j9ze83mfdo93wc49uth4j369r9o4apn2b-s3alias"
)
S3_DATA_DELTA_PATH = f"s3a://{S3_DELTA_ACCESS_POINT_ALIAS}/news_collection/"

# transform process
update_df = source_df.dropDuplicates(["publish_date", "url", "title"])

update_df = update_df.withColumn(
    "keyword", F.regexp_extract(F.col("keyword"), "[가-힣a-zA-Z0-9]+", 0)
).withColumn("publish_date", F.to_date(F.col("publish_date"), "yyyy.MM.dd"))


# DeltaTable 인스턴스 생성
try:
    delta_table = DeltaTable.forPath(spark, S3_DATA_DELTA_PATH)  # noqa: F405
except AnalysisException:
    # Create DeltaTable instances
    update_df.write.mode("overwrite").format("delta").save(S3_DATA_DELTA_PATH)

# Perform Upsert
# Delta table에 update_df를 Update or Insert하는 과정
delta_table.alias("old_data").merge(
    update_df.alias("new_data"), "old_data.url = new_data.url"
).whenMatchedUpdate(
    set={
        "publish_date": "new_data.publish_date",
        "publisher": "new_data.publisher",
        "title": "new_data.title",
        "description": "new_data.description",
    }
).whenNotMatchedInsertAll().execute()
