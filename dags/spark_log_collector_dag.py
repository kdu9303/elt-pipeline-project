# -*- coding: utf-8 -*-
import logging
import pendulum
from datetime import timedelta
from airflow.decorators import dag
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
    schedule_interval="*/5 * * * *",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 14, tz="Asia/Seoul"),
    tags=["spark_log_stream"],
)
def scrape_spark_log_data():
    """
    ### Dag Documentation
    목적:
        SPARK 생성 Log 정보의 pipeline 구성

    Task 순서:
        1. Airflow Server에서 Spark driver server로 py file 스크립트를 전송한다.
        2. Spark remote server에 py파일이 도착했는지 확인한다.
        3. Livy rest api를 통해 spark job을 할당한다.
    """

    # SSH File Transfer
    file_name = "spark_job_spark_log_stream_collector.py"
    local_file_path = f"/opt/airflow/dags/modules/log_stream/{file_name}"
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
        transter_python_script  # noqa: W503
        >> spark_script_file_checker  # noqa: W503
        >> run_spark_batch_job  # noqa: W503
    )


dag = scrape_spark_log_data()
