# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
import os
import configparser


def getAWSKeys(profile):
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


awsKeys = getAWSKeys("default")
Access_key_ID = awsKeys["aws_access_key_id"]
Secret_access_key = awsKeys["aws_secret_access_key"]

spark = (
    SparkSession.builder.master("yarn")
    .appName("test")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "2g")
    .config("spark.yarn.am.memory", "2g")
    .config("spark.executor.cores", 3)
    .config("spark.executor.instances", 1)
    .getOrCreate()
)

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
