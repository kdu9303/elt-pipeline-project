# -*- coding: utf-8 -*-
import logging
import pendulum
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from modules.producer import MessageProducer
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
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
    S3_data_upload_checker = S3KeySensor(
        task_id="S3_data_upload_checker",
        bucket_key=f"s3://etl-project-bucket-20220817/air_quality/air_quality-s3-sink/{partition_folder_name}/*",
        wildcard_match=True,
        aws_conn_id="aws_connection",
        timeout=600,
        poke_interval=60,
    )

    # task flow
    (produce_data_to_broker() >> S3_data_upload_checker)  # noqa: W503


dag = scrape_air_quality_data()
