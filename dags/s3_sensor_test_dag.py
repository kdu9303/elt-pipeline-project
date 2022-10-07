# -*- coding: utf-8 -*-
import logging
import pendulum
from datetime import datetime
from airflow.decorators import dag
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
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
    tags=["S3 sensor test dag"],
    catchup=False,
)
def s3_sensor_test():
    """
    ### Dag Documentation
    S3 bucket에 해당 일자 자료가 들어왔는지 확인하는 dag입니다.
    """
    # 해당일자 폴더
    folder_name = datetime.now().strftime("%Y-%m-%d")

    data_upload_check_sensor = S3KeySensor(
        task_id="S3_data_upload_check_sensor",
        bucket_key=f"s3://etl-project-bucket-20220817/news_collection/news_collection-s3-sink/{folder_name}/*",
        wildcard_match=True,
        aws_conn_id="aws_connection",
        timeout=18 * 60 * 60,
        poke_interval=60,
    )

    data_upload_check_sensor


dag = s3_sensor_test()
