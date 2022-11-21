# -*- coding: utf-8 -*-
import os
import configparser
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
    .config(
        "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite",
        "true",
    )
    .config(
        "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact",
        "true",
    )
    .config(
        "spark.databricks.delta.properties.defaults.compatibility.symlinkFormatManifest.enabled",
        "true",
    )
    .getOrCreate()
)

# delta lake package path
spark.sparkContext.addPyFile(
    "/home/ec2-user/spark/jars/delta-core_2.12-2.0.0.jar"
)

# log4j property file
spark.sparkContext.addFile("/home/ec2-user/spark/conf/log4j.properties")

# spark session을 생성한 이후 delta lake 모듈을 불러올 수 있음
from delta.tables import *  # noqa: E402, F403, F405

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

table_name = "census_data_collection"

# source path 설정
S3_SOURCE_ACCESS_POINT_ALIAS = (
    "etl-project-bucket-os3osysqodthpasbb5yqre5ot7jeaapn2a-s3alias"
)

# 날짜 파티션 단위로 데이터를 불러올때(용량이 큰 경우)
# SORUCE_PROCESS_DATE = datetime.now().strftime("%Y-%m-%d")
# S3_DATA_SOURCE_PATH = f"s3a://{S3_SOURCE_ACCESS_POINT_ALIAS}/{table_name}/{table_name}-s3-sink/{SORUCE_PROCESS_DATE}/*"

S3_DATA_SOURCE_PATH = (
    f"s3a://{S3_SOURCE_ACCESS_POINT_ALIAS}/{table_name}/{table_name}-s3-sink/*"
)

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
S3_DATA_DELTA_PATH = f"s3a://{S3_DELTA_ACCESS_POINT_ALIAS}/{table_name}/"

# -------------------------------------------------------------------
# Transfom Process
# -------------------------------------------------------------------

select_column_list = ["PRD_DE", "C1", "C1_NM", "C1_NM_ENG", "TBL_ID", "DT"]

update_df = source_df.select(select_column_list)

update_df = update_df.distinct()

update_df = (
    update_df.withColumn("PRD_DE", F.expr("PRD_DE || '01'"))
    .withColumn("PRD_DE", F.to_date(F.col("PRD_DE"), "yyyyMMdd"))
    .withColumn("DT", F.col("DT").cast("int"))
    .withColumn("C1_SIDO", F.substring(F.col("C1"), 1, 2))
)

# 상위 행정지역 필터
city_df = update_df.filter(F.length("C1") == 2)

joined_df = (
    update_df.alias("a")
    .join(
        city_df.alias("b"),
        (F.col("a.PRD_DE") == F.col("b.PRD_DE"))
        & (F.col("a.C1_SIDO") == F.col("b.C1_SIDO")),  # noqa: W503
        "left",
    )
    .select("a.*", F.col("b.C1_NM").alias("C1_NM_SIDO"))
)

# DeltaTable 인스턴스 생성
try:
    delta_table = DeltaTable.forPath(spark, S3_DATA_DELTA_PATH)  # noqa: F405

except AnalysisException:
    # Create DeltaTable instances
    (
        update_df.write.mode("overwrite")
        .format("delta")
        .option("targetFileSize", "104857600")
        .save(S3_DATA_DELTA_PATH)
    )
    delta_table = DeltaTable.forPath(spark, S3_DATA_DELTA_PATH)  # noqa: F405

    # SQL engine에서 Delta table을 인식하기 위해 manifest필요
    delta_table.generate("symlink_format_manifest")

# -------------------------------------------------------------------
# Perform Upsert
# -------------------------------------------------------------------
# Delta table에 update_df를 Update or Insert하는 과정
delta_table.alias("old_data").merge(
    joined_df.alias("new_data"),
    "old_data.PRD_DE = new_data.PRD_DE AND old_data.C1 = new_data.C1",
).whenMatchedUpdate(
    set={
        "C1_NM_SIDO": "new_data.C1_NM_SIDO",
        "C1_NM": "new_data.C1_NM",
        "C1_NM_ENG": "new_data.C1_NM_ENG",
        "TBL_ID": "new_data.TBL_ID",
        "DT": "new_data.DT",
    }
).whenNotMatchedInsertAll().execute()

spark.stop()
