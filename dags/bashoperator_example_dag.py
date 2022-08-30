import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
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
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    tags=["test dag"],
)
def print_python_version():
    """
    ### Dag Documentation
    python version을 프린트하는 test dag 입니다.
    """
    execute_cmd = BashOperator(
        task_id="shell_execute", bash_command="echo python --version"
    )

    execute_cmd


dag = print_python_version()
