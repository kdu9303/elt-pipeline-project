# -*- coding: utf-8 -*-
import logging
import pendulum
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from modules.producer import MessageProducer
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from modules.spark_job_livy_custom_operator import SparkSubmitOperator
from modules.kosis_statistics.module_census_data_collector import (
    CensusDataScraper,
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
    schedule_interval="45 22 2-4 * *",
    catchup=False,
    start_date=pendulum.datetime(2022, 10, 1, tz="Asia/Seoul"),
    tags=["census_data_collection"],
)
def scrape_census_data():
    """
    ### Dag Documentation
    목적:
        KOSIS(통계청) API 시군구별 인구수 정보 pipeline 구성

    Task 순서:
        1. KOSIS API에서 인구수 자료를 호출하여 Kafka producer로 넘긴다.
        2. S3 bucket에 해당일자 partition에 자료가 도착했는지 확인한다.
        3. Airflow Server에서 Spark driver server로 py file 스크립트를 전송한다.
        4. Spark remote server에 py파일이 도착했는지 확인한다.
        5. Livy rest api를 통해 spark job을 할당한다.
    """

    @task
    def produce_data_to_broker() -> None:

        # producer config
        topic = "census_data_collection-s3-sink"
        key_schema_path = (
            "/opt/airflow/dags/modules/avro_schema/census_data_schema_key.avsc"
        )
        value_schema_path = "/opt/airflow/dags/modules/avro_schema/census_data_schema_value.avsc"
        message_producer = MessageProducer(
            topic, key_schema_path, value_schema_path
        )

        # 날짜 범위 지정
        current_month = date.today().replace(day=1)
        start_month = current_month + relativedelta(months=-3)

        census_api_collector = CensusDataScraper()

        data = census_api_collector.get_census_data(
            startPrdDe=start_month, endPrdDe=current_month
        )

        try:
            for row in data:
                message_producer.produce(row)

            logging.info("message sent successfully...")

        except Exception as e:
            raise AirflowException(e)

    # S3 Sensor
    partition_folder_name = datetime.now().strftime("%Y-%m-%d")
    table_name = "census_data_collection"
    S3_data_upload_checker = S3KeySensor(
        task_id="S3_data_upload_checker",
        bucket_key=f"s3://etl-project-bucket-20220817/{table_name}/{table_name}-s3-sink/{partition_folder_name}/*",
        wildcard_match=True,
        aws_conn_id="aws_connection",
        timeout=120,
        poke_interval=60,
    )

    # SSH File Transfer
    file_name = "spark_job_census_data_collector.py"
    local_file_path = f"/opt/airflow/dags/modules/kosis_statistics/{file_name}"
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


dag = scrape_census_data()
