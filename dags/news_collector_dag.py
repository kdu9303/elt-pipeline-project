# -*- coding: utf-8 -*-
import pytz
import logging
import pendulum
from datetime import datetime, timedelta
from airflow.decorators import dag, task
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
    tags=["scrape_data"],
)
def scrape_news_data():
    """
    ### Dag Documentation
    네이버 뉴스에서 특정 키워드를 크롤링하여 s3에 저장합니다.
    """
    keywords = ["인천세종병원", "심장내과", "흉부외과", "환자경험평가"]

    @task
    def print_data(keyword: str):

        current_date = datetime.now(pytz.timezone("Asia/Seoul"))
        start_date = current_date - timedelta(days=2)
        end_date = current_date - timedelta(days=1)

        scraper = NaverNewsScraper(keyword)
        news = scraper.run(start_date, end_date)

        for article in news:
            logger.info(article)

    print_news = print_data.expand(keywords)


dag = scrape_news_data()
