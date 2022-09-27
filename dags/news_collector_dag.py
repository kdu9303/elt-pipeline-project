# -*- coding: utf-8 -*-
import logging
import pendulum
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.glue_crawler import (
    GlueCrawlerOperator,
)
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

    @task
    def produce_data_to_broker() -> None:

        # producer config
        topic = "news-collecion-s3-sink"
        key_schema_path = "/opt/airflow/dags/modules/avro_schema/news_collection_schema_key.avsc"
        value_schema_path = "/opt/airflow/dags/modules/avro_schema/news_collection_schema_value.avsc"
        message_producer = MessageProducer(
            topic, key_schema_path, value_schema_path
        )

        keywords = ["인천세종병원", "부천세종병원", "심장내과", "흉부외과", "환자경험평가", "코로나19"]

        # 날짜 범위 지정
        current_date = datetime.now(pendulum.timezone("Asia/Seoul"))
        start_date = current_date - timedelta(days=7)
        end_date = current_date - timedelta(days=1)

        try:
            for keyword in keywords:
                scraper = NaverNewsScraper(keyword)
                data = scraper.run(start_date, end_date)

                for row in data:
                    message_producer.produce(row)

            logging.info("message sent successfully...")
        except Exception as e:
            raise AirflowException(e)

    glue_crawler_config = {"Name": "elt-project-data-crawler"}
    run_glue_crawl_s3 = GlueCrawlerOperator(
        task_id="run_glue_crawl_s3",
        aws_conn_id="aws_connection",
        config=glue_crawler_config,
        wait_for_completion=True,
    )

    # task flow
    produce_data_to_broker() >> run_glue_crawl_s3


dag = scrape_news_data()
