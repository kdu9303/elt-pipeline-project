# -*- coding: utf-8 -*-
import pendulum
from airflow.decorators import dag
from airflow.providers.apache.livy.operators.livy import LivyOperator
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
    run_spark_batch_job = LivyOperator(
        name="run_spark_batch_job",  # job name
        file="/opt/airflow/dags/modules/spark_job_teset.py",
        livy_conn_id="livy_connection",
        task_id="run_spark_batch_job",
    )

    run_spark_batch_job


dag = spark_job_http_request_test()
