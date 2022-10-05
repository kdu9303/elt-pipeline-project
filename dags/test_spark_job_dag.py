# -*- coding: utf-8 -*-
import pendulum
from airflow.decorators import dag
from airflow_livy.batch import LivyBatchOperator
from datetime import timedelta


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
    run_spark_batch_job = LivyBatchOperator(
        name="run_spark_batch_job",  # job name
        file="file:///opt/airflow/dags/modules/spark_job_teset.py",
        verify_in="yarn",
        task_id="run_spark_batch_job",
    )

    run_spark_batch_job


dag = spark_job_http_request_test()
