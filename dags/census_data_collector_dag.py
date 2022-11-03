# -*- coding: utf-8 -*-
import logging
import pendulum
import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from modules.producer import MessageProducer
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
    schedule_interval=None,
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    tags=["census_data_collection"],
)
def collect_census_data():
    """
    ### Dag Documentation
    통계청api로 시군구 단위별 인구수 data를 받아서 s3에 저장한다.
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
        current_month = datetime.date.today().replace(day=1)
        start_month = current_month + relativedelta(months=-6)

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

    # task flow
    produce_data_to_broker()


dag = collect_census_data()
