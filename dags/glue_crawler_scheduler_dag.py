# -*- coding: utf-8 -*-
import logging
import pendulum
from datetime import timedelta
from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.glue_crawler import (
    GlueCrawlerOperator,
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
    schedule_interval="30 15,22 * * *",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 15, tz="Asia/Seoul"),
    tags=["glue_crawler"],
)
def trigger_glue_crawler():
    """
    ### Dag Documentation
    목적:
        Glue Crawler 스케쥴러 실행

    Task 순서:
        Bronze Bucker의 Glue Crawler를 작동 시킨다.
        Silver Bucker의 Glue Crawler를 작동 시킨다.
    """
    crawler_config = {"Name": "elt-project-data-crawler"}
    run_crawler1 = GlueCrawlerOperator(
        task_id="run_crawler",
        aws_conn_id="aws_connection",
        config=crawler_config,
        wait_for_completion=True,
    )

    crawler_config = {"Name": "delta-lake-crawler"}
    run_crawler2 = GlueCrawlerOperator(
        task_id="run_crawler",
        aws_conn_id="aws_connection",
        config=crawler_config,
        wait_for_completion=True,
    )
    # task flow
    run_crawler1  # noqa: W503
    run_crawler2  # noqa: W503


dag = trigger_glue_crawler()
