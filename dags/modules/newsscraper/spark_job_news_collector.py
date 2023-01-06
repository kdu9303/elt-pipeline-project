# -*- coding: utf-8 -*-
import os
import sys
import configparser
from typing import Dict, List
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException


class SparkClass:
    def __init__(self, app_name: str) -> None:
        self.app_name = app_name

        # source path 설정
        self.s3_source_access_point_alias = (
            "etl-project-bucket-os3osysqodthpasbb5yqre5ot7jeaapn2a-s3alias"
        )
        self.s3_data_source_path = f"s3a://{self.s3_source_access_point_alias}/{table_name}/{table_name}-s3-sink/*"

        # delta target path 설정
        self.s3_delta_access_point_alias = (
            "etl-project-bucket-d-5j9ze83mfdo93wc49uth4j369r9o4apn2b-s3alias"
        )
        self.s3_delta_path = (
            f"s3a://{self.s3_delta_access_point_alias}/{table_name}/"
        )

    def start_spark(self, config: Dict) -> SparkSession:
        try:

            def get_aws_keys(profile: str) -> Dict[str, str]:
                aws_creds = {}
                conf = configparser.ConfigParser()
                conf.read(
                    os.path.expanduser("/home/ec2-user/.aws/credentials")
                )
                if profile in conf.sections():
                    aws_creds["aws_access_key_id"] = conf.get(
                        profile, "aws_access_key_id"
                    )
                    aws_creds["aws_secret_access_key"] = conf.get(
                        profile, "aws_secret_access_key"
                    )
                return aws_creds

            def create_builder(app_name: str) -> SparkSession.Builder:

                builder = SparkSession.builder.appName(app_name).master("yarn")
                return builder

            def create_session(builder: SparkSession.Builder) -> SparkSession:
                return builder.getOrCreate()

            def set_session_config(
                spark: SparkSession, spark_conf: List[tuple]
            ) -> None:
                spark.sparkContext._conf.setAll(spark_conf)

            def set_hadoop_config(
                spark: SparkSession,
                Access_key_ID: str,
                Secret_access_key: str,
                hadoop_conf: List[tuple],
            ) -> None:

                hadoop = spark.sparkContext._jsc.hadoopConfiguration()

                # aws credentials
                hadoop.set("fs.s3a.access.key", Access_key_ID),
                hadoop.set("fs.s3a.secret.key", Secret_access_key),
                hadoop.set(
                    "fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com"
                )

                for key, value in hadoop_conf:
                    hadoop.set(key, value)

            def add_jar_file(spark: SparkSession) -> None:

                file_path_list = [
                    "/home/ec2-user/spark/jars/delta-core_2.12-2.0.0.jar"
                ]
                for file in file_path_list:
                    spark.sparkContext.addPyFile(file)

            spark_conf = config.get("spark_conf", {})
            hadoop_conf = config.get("hadoop_conf", {})
            awsKeys = get_aws_keys("default")
            Access_key_ID = awsKeys["aws_access_key_id"]
            Secret_access_key = awsKeys["aws_secret_access_key"]

            builder = create_builder(self.app_name)
            spark = create_session(builder)
            set_session_config(spark, spark_conf)
            set_hadoop_config(
                spark, Access_key_ID, Secret_access_key, hadoop_conf
            )
            add_jar_file(spark)

            return spark

        except Exception as e:
            print(
                "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                type(e).__name__,
                e,
            )

    def import_source_data(
        self, spark: SparkSession, table_name: str, format_type: str
    ) -> DataFrame:
        """
        Data Source로 부터 DataFrame을 생성한다.
        필요시 Format 형식에 맞는 함수 추가
        """

        def source_from_avro(
            spark: SparkSession, format_type: str, datapath: str
        ) -> DataFrame:

            source_df = (
                spark.read.format(format_type)
                .option("header", "true")
                .option("inferSchema", "true")
                .option("mode", "FAILFAST")
                .load(datapath)
            )
            return source_df

        def source_from_csv(
            spark: SparkSession, format_type: str, datapath: str
        ) -> DataFrame:

            source_df = (
                spark.read.format(format_type)
                .option("header", "true")
                .option("mode", "DROPMALFORMED")
                .load(datapath)
            )
            return source_df

        def source_from_json(
            spark: SparkSession, format_type: str, datapath: str
        ) -> DataFrame:

            source_df = (
                spark.read.format(format_type)
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .load(datapath)
            )
            return source_df

        def source_from_text(
            spark: SparkSession, format_type: str, datapath: str
        ) -> DataFrame:

            source_df = spark.read.format(format_type).load(datapath)
            return source_df

        if format_type == "avro":
            return source_from_avro(
                spark, format_type, self.s3_data_source_path
            )
        elif format_type == "csv":
            return source_from_csv(
                spark, format_type, self.s3_data_source_path
            )
        elif format_type == "json":
            return source_from_json(
                spark, format_type, self.s3_data_source_path
            )
        elif format_type == "txt" or format_type == "text":
            return source_from_text(spark, "text", self.s3_data_source_path)

    def create_delta_table(self, dataframe: DataFrame) -> None:
        (
            dataframe.write.mode("overwrite")
            .format("delta")
            .option("mergeSchema", "true")
            .option("targetFileSize", "104857600")
            .save(self.s3_delta_path)
        )

    def import_delta_table(
        self, spark: SparkSession, table_name: str, dataframe: DataFrame = None
    ) -> DataFrame:
        """
        Delta Table을 불러온다.
        최초 생성시에는 DataFrame이 필요
        """
        # spark session을 생성한 이후 delta lake 모듈을 불러올 수 있음
        from delta.tables import DeltaTable

        try:
            delta_table = DeltaTable.forPath(spark, self.s3_delta_path)
        except AnalysisException:
            # Create DeltaTable instances
            self.create_delta_table(dataframe)
            delta_table = DeltaTable.forPath(spark, self.s3_delta_path)

            # SQL engine에서 Delta table을 인식하기 위해 manifest필요
            delta_table.generate("symlink_format_manifest")

        return delta_table


# -------------------------------------------------------------------
# Transfom Functions
# -------------------------------------------------------------------
def transform_data(source_dataframe: DataFrame) -> DataFrame:
    update_df = source_dataframe.distinct()

    update_df = update_df.withColumn(
        "keyword", F.regexp_extract(F.col("keyword"), "[가-힣a-zA-Z0-9]+", 0)
    ).withColumn(
        "publish_date", F.to_date(F.col("publish_date"), "yyyy.MM.dd")
    )
    return update_df


def merge_data(
    spark: SparkSession, table_name: str, dataframe: DataFrame
) -> None:
    delta_table = sparkclass.import_delta_table(spark, table_name, dataframe)

    # remove possible duplicate rows in delta table
    delta_table.toDF().createOrReplaceTempView(table_name)
    duplicate_rows = (
        (
            spark.sql(
                f"SELECT *, ROW_NUMBER() OVER (PARTITION BY url, keyword ORDER BY publish_date DESC) row_num FROM {table_name}"
            )
        )
        .filter(F.col("row_num") > 1)
        .drop("row_num")
        .distinct()
    )
    try:
        # 중복 데이터 제거
        delta_table.alias("main").merge(
            duplicate_rows.alias("duplicate_rows"),
            "main.url = duplicate_rows.url AND main.keyword = duplicate_rows.keyword",
        ).whenMatchedDelete().execute()

        # Merge
        delta_table.alias("old_data").merge(
            update_df.alias("new_data"),
            "old_data.url = new_data.url AND old_data.keyword = new_data.keyword",
        ).whenNotMatchedInsertAll().execute()
    except AnalysisException:
        sparkclass.create_delta_table(spark, dataframe)


if __name__ == "__main__":
    config = {
        "spark_conf": [
            ("spark.submit.deployMode", "cluster"),
            ("spark.driver.memory", "2g"),
            ("spark.driver.cores", 1),
            ("spark.executor.instances", 2),
            ("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.4"),
            ("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0"),
            (
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            ),
            (
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            ),
            (
                "spark.delta.logStore.class",
                "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
            ),
            ("spark.databricks.delta.schema.autoMerge.enabled", "true"),
            (
                "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite",
                "true",
            ),
            (
                "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact",
                "true",
            ),
            (
                "spark.databricks.delta.properties.defaults.compatibility.symlinkFormatManifest.enabled",
                "true",
            ),
        ],
        "hadoop_conf": [
            ("com.amazonaws.services.s3.enableV4", "true"),
            ("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"),
            (
                "fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            ),
            ("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A"),
        ],
    }

    # -------------------------------------------------------------------
    # Data setting
    # -------------------------------------------------------------------
    table_name = "news_collection"

    sparkclass = SparkClass(
        app_name="News_Collection_Data_Transfrom"
    )  # spark history에서 구분자 사용
    spark = sparkclass.start_spark(config)

    source_df = sparkclass.import_source_data(spark, table_name, "avro")

    # -------------------------------------------------------------------
    # Transfom Process
    # -------------------------------------------------------------------
    update_df = transform_data(source_df)
    merge_data(spark, table_name, update_df)

    spark.stop()
