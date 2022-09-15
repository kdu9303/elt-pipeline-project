# -*- coding: utf-8 -*-
import pytz
import logging
import pendulum
from typing import List, Dict, TypedDict
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from modules.producer import MessageProducer
from modules.newsscraper.module_news_collector import NaverNewsScraper


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
    tags=["news_collection"],
)
def scrape_news_data():
    """
    ### Dag Documentation
    네이버 뉴스에서 특정 키워드를 크롤링하여 s3에 저장합니다.
    """

    # return값 정의
    news_type = TypedDict(
        "news_type",
        {
            "publish_date": datetime,
            "publisher": str,
            "title": str,
            "url": str,
            "description": str,
            "keyword": str,
        },
    )

    @task
    def extract_data_from_naver_news() -> List[news_type]:

        keywords = ["인천세종병원", "심장내과", "흉부외과", "환자경험평가"]

        current_date = datetime.now(pytz.timezone("Asia/Seoul"))
        start_date = current_date - timedelta(days=3)
        end_date = current_date - timedelta(days=1)

        data = []
        for keyword in keywords:
            scraper = NaverNewsScraper(keyword)
            data.append(scraper.run(start_date, end_date))

        return data

    @task
    def produce_data_to_broker(data: Dict) -> None:
        topic = "news-collecion-s3-sink"
        key_schema_path = "/opt/airflow/dags/modules/avro_schema/news_collection_schema_key.avsc"
        value_schema_path = "/opt/airflow/dags/modules/avro_schema/news_collection_schema_value.avsc"
        message_producer = MessageProducer(
            topic, key_schema_path, value_schema_path
        )

        for row in data:
            message_producer.produce(row)
        logging.info("message sent successfully...")

    # task flow
    news_data = extract_data_from_naver_news()
    send_to_broker = produce_data_to_broker(news_data)


dag = scrape_news_data()
