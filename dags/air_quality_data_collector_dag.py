# -*- coding: utf-8 -*-
import logging
import pendulum
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from modules.producer import MessageProducer
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.amazon.aws.operators.glue_crawler import (
    GlueCrawlerOperator,
)
from modules.spark_job_livy_custom_operator import SparkSubmitOperator
from modules.air_quality_statistics.module_air_quality_collector import (
    AirQualityDataScraper,
)


logger = logging.getLogger()

default_args = {
    "owner": "airflow",
    "retries": 1,
    "email": ["zheros9303@gmail.com"],
    "retry_delay": timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 2, tz="Asia/Seoul"),
    tags=["air_qality"],
)
def scrape_air_quality_data():
    """
    ### Dag Documentation
    목적:
        OPEN API 시군구별 실시간 대기의 질 정보 pipeline 구성

    Task 순서:
        1. OPEN API에서 지역별 자료를 호출하여 Kafka producer로 넘긴다.
        2. S3 bucket에 해당일자 partition에 자료가 도착했는지 확인한다.
        3. Airflow Server에서 Spark driver server로 py file 스크립트를 전송한다.
        4. Spark remote server에 py파일이 도착했는지 확인한다.
        5. Livy rest api를 통해 spark job을 할당한다.
        6. Glue Crawler를 작동 시킨다.
    """

    # Producer task
    @task
    def produce_data_to_broker() -> None:

        # producer config
        topic = "air_quality-s3-sink"
        key_schema_path = (
            "/opt/airflow/dags/modules/avro_schema/air_quality_schema_key.avsc"
        )
        value_schema_path = "/opt/airflow/dags/modules/avro_schema/air_quality_schema_value.avsc"
        message_producer = MessageProducer(
            topic, key_schema_path, value_schema_path
        )

        city_list = ["서울", "인천", "경기"]

        try:
            for city in city_list:
                air_quality_scraper = AirQualityDataScraper()
                data = air_quality_scraper.get_air_quality_data(sidoName=city)

                for row in data:
                    message_producer.produce(row)

            logger.info("message sent successfully...")
        except Exception as e:
            raise AirflowException(e)

    # S3 Sensor
    partition_folder_name = datetime.now().strftime("%Y-%m-%d")
    table_name = "air_quality"
    S3_data_upload_checker = S3KeySensor(
        task_id="S3_data_upload_checker",
        bucket_key=f"s3://etl-project-bucket-20220817/{table_name}/{table_name}-s3-sink/{partition_folder_name}/*",
        wildcard_match=True,
        aws_conn_id="aws_connection",
        timeout=120,
        poke_interval=60,
    )

    # SSH File Transfer
    file_name = "spark_job_air_quality_data_collector.py"
    local_file_path = (
        f"/opt/airflow/dags/modules/air_quality_statistics/{file_name}"
    )
    remote_file_path = f"/home/ec2-user/spark-data/{file_name}"

    transter_python_script = SFTPOperator(
        task_id="transter_python_script",
        ssh_conn_id="spark_master_host_connection",
        local_filepath=local_file_path,
        remote_filepath=remote_file_path,
        operation="put",
        create_intermediate_dirs=True,
    )

    # Remote File Sensor
    spark_script_file_checker = SFTPSensor(
        task_id="spark_script_file_checker",
        sftp_conn_id="spark_master_host_connection",
        path=remote_file_path,
        timeout=120,
        poke_interval=10,
    )

    run_spark_batch_job = SparkSubmitOperator(
        task_id="run_spark_batch_job",
        livy_conn_id="livy_connection",
        file_name=file_name,
        polling_interval=30,
        delete_session=False,
    )

    crawler_config = {"Name": "delta-lake-crawler"}
    run_crawler = GlueCrawlerOperator(
        task_id="run_crawler",
        aws_conn_id="aws_connection",
        config=crawler_config,
        wait_for_completion=True,
    )

    # task flow
    (
        produce_data_to_broker()
        >> S3_data_upload_checker  # noqa: W503
        >> transter_python_script  # noqa: W503
        >> spark_script_file_checker  # noqa: W503
        >> run_spark_batch_job  # noqa: W503
        >> run_crawler  # noqa: W503
    )


dag = scrape_air_quality_data()
