# -*- coding: utf-8 -*-
import logging
import pendulum
from datetime import datetime
from time import sleep
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.apache.livy.hooks.livy import BatchState, LivyHook
from airflow.providers.sftp.operators.sftp import SFTPOperator
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
def spark_job_http_request_test(file_name):
    """
    ### Dag Documentation
    Livy rest api를 통해 spark job을 할당해주는 dag입니다.
    """
    remote_file_path = f"/home/ec2-user/spark-data/{file_name}"

    transter_python_script = SFTPOperator(
        task_id="transter_python_script",
        ssh_conn_id="spark_master_host_connection",
        local_filepath="/opt/airflow/dags/modules/{file_name}",
        remote_filepath=remote_file_path,
        operation="put",
        create_intermediate_dirs=True,
    )

    @task
    def run_spark_batch_job():
        postfix = datetime.now().strftime("%Y%m%d-%H%M%S")
        # 배치 이름이 겹치면 오류
        batch_name = f"run-{file_name}-spark_job-{postfix}"
        spark_host_local_file_path = f"file:{remote_file_path}"

        livy_hook = LivyHook(livy_conn_id="livy_connection")

        batch_id = livy_hook.post_batch(
            name=batch_name, file=spark_host_local_file_path
        )

        state = livy_hook.get_batch_state(batch_id)

        while state not in livy_hook.TERMINAL_STATES:
            logger.debug(
                f"Batch with id {batch_id} is in state: {state.value}"
            )
            # 10초에 한번씩 status check
            sleep(10)
            state = livy_hook.get_batch_state(batch_id)

        logger.info(
            "Batch with id {batch_id} terminated with state: {state.value}"
        )

        if state != BatchState.SUCCESS:
            raise AirflowException(f"Batch {batch_id} did not succeed")

        if batch_id is not None:
            livy_hook.delete_batch(batch_id)

    transter_python_script >> run_spark_batch_job()


dag = spark_job_http_request_test("spark_job_test.py")
