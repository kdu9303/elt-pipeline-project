# -*- coding: utf-8 -*-
import logging
import pendulum
from airflow.decorators import dag, task
from datetime import timedelta
from modules.producer import MessageProducer

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
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    tags=["test_producer"],
)
def test_producer():
    """
    ### Dag Documentation
    Testing Kafka producer dag
    """
    topic = "test"
    # path는 docker 컨테이너 내부 경로로 설정
    key_schema_path = (
        "/opt/airflow/dags/modules/avro_schema/test_schema_key.avsc"
    )
    value_schema_path = (
        "/opt/airflow/dags/modules/avro_schema/test_schema_value.avsc"
    )

    @task
    def send_message():

        message_producer = MessageProducer(
            topic, key_schema_path, value_schema_path
        )

        data = {"name": "abc2", "email": "abc2@example.com"}

        message_producer.produce(data)
        logging.info("message sent successfully...")

    send_message()


dag = test_producer()
