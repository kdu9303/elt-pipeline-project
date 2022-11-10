# -*- coding: utf-8 -*-
import logging
import pendulum
from airflow.decorators import dag
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from modules.spark_job_livy_custom_operator import SparkSubmitOperator
from datetime import timedelta

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
    start_date=pendulum.datetime(2022, 1, 1, tz="Asia/Seoul"),
    tags=["spark job test dag"],
    catchup=False,
)
def spark_job_http_request_test():
    """
    ### Dag Documentation
    Livy rest api를 통해 spark job을 할당해주는 dag입니다.
    """
    # spark job script
    file_name = "spark_job_test.py"

    # Spark Driver로 전송할 Script
    local_file_path = f"/opt/airflow/dags/modules/{file_name}"
    remote_file_path = f"/home/ec2-user/spark-data/{file_name}"

    # Airflow Server에서 Spark driver Server로 스크립트 전송
    transter_python_script = SFTPOperator(
        task_id="transter_python_script",
        ssh_conn_id="spark_master_host_connection",
        local_filepath=local_file_path,
        remote_filepath=remote_file_path,
        operation="put",
        create_intermediate_dirs=True,
    )

    # Remote File Sensor
    wait_for_input_file = SFTPSensor(
        task_id="wait_for_input_file",
        sftp_conn_id="spark_master_host_connection",
        path=remote_file_path,
        poke_interval=10,
    )

    run_spark_batch_job = SparkSubmitOperator(
        task_id="run_spark_batch_job",
        livy_conn_id="livy_connection",
        file_name=file_name,
    )

    transter_python_script >> wait_for_input_file >> run_spark_batch_job


dag = spark_job_http_request_test()
