# -*- coding: utf-8 -*-
import logging
import pendulum
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from modules.producer import MessageProducer
from modules.newsscraper.module_news_collector import NaverNewsScraper
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from modules.spark_job_livy_custom_operator import SparkSubmitOperator

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
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    tags=["news_collection"],
)
def scrape_news_data():
    """
    ### Dag Documentation
    목적:
        네이버 뉴스 자료 pipeline 구성

    Task 순서:
        1. 네이버 뉴스에서 특정 키워드를 크롤링하여 Kafka producer로 넘긴다.
        2. S3 bucket에 해당일자 partition에 자료가 도착했는지 확인한다.
        3. Airflow Server에서 Spark driver server로 py file 스크립트를 전송한다.
        4. Spark remote server에 py파일이 도착했는지 확인한다.
        5. Livy rest api를 통해 spark job을 할당한다.
    """

    # Producer task
    @task
    def produce_data_to_broker() -> None:

        # producer config
        topic = "news_collection-s3-sink"
        key_schema_path = "/opt/airflow/dags/modules/avro_schema/news_collection_schema_key.avsc"
        value_schema_path = "/opt/airflow/dags/modules/avro_schema/news_collection_schema_value.avsc"
        message_producer = MessageProducer(
            topic, key_schema_path, value_schema_path
        )

        keywords = ["인천세종병원", "부천세종병원", "심장내과", "흉부외과", "환자경험평가", "코로나19"]

        # 날짜 범위 지정
        current_date = datetime.now(pendulum.timezone("Asia/Seoul"))
        start_date = current_date - timedelta(days=3)
        end_date = current_date - timedelta(days=1)

        try:
            for keyword in keywords:
                scraper = NaverNewsScraper(keyword)
                data = scraper.run(start_date, end_date)

                for row in data:
                    message_producer.produce(row)

            logger.info("message sent successfully...")
        except Exception as e:
            raise AirflowException(e)

    # S3 Sensor
    partition_folder_name = datetime.now().strftime("%Y-%m-%d")
    S3_data_upload_checker = S3KeySensor(
        task_id="S3_data_upload_checker",
        bucket_key=f"s3://etl-project-bucket-20220817/news_collection/news_collection-s3-sink/{partition_folder_name}/*",
        wildcard_match=True,
        aws_conn_id="aws_connection",
        timeout=120,
        poke_interval=60,
    )

    # SSH File Transfer
    file_name = "spark_job_news_collector.py"
    local_file_path = f"/opt/airflow/dags/modules/newsscraper/{file_name}"
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

    # task flow
    (
        produce_data_to_broker()
        >> S3_data_upload_checker  # noqa: W503
        >> transter_python_script  # noqa: W503
        >> spark_script_file_checker  # noqa: W503
        >> run_spark_batch_job  # noqa: W503
    )


dag = scrape_news_data()
