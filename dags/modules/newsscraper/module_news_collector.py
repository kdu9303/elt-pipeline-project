# -*- coding: utf-8 -*-
import time
import pytz
import urllib
import logging
import requests
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

logger = logging.getLogger()


class NaverNewsScraper:
    def __init__(self, keyword: str):
        self.NAVER_BASE_URL = "https://search.naver.com/search.naver?"
        self.keyword = f'"{keyword}"'

    def fetcher(self, session: requests.Session, url: str):
        headers = {"User-Agent": UserAgent(use_cache_server=True).random}

        with session.get(url, headers=headers) as response:
            if response.status_code == 200:
                return response.text

    ##############################################################################
    # 초기 입력 변수 설정
    ##############################################################################

    def parse_keyword(self) -> str:
        return urllib.parse.urlencode(
            {"query": self.keyword}, encoding="utf-8"
        )

    def get_date_range(
        self, start_date: datetime, end_date: datetime
    ) -> list[str]:

        # 날짜 제너레이터 생성
        def daterange(start_date: datetime, end_date: datetime) -> list[str]:

            for n in range(int((end_date - start_date).days) + 1):
                yield start_date + timedelta(n)

        date_range = []
        for single_date in daterange(start_date, end_date):
            date_range.append(single_date.strftime("%Y.%m.%d"))

        return date_range

    ##############################################################################
    # html 파싱
    ##############################################################################

    def parse_html(
        self, fetcher_session: requests.Session, fetcher_url: str
    ) -> BeautifulSoup:

        html_text = self.fetcher(fetcher_session, fetcher_url)
        html_parser = BeautifulSoup(html_text, "html.parser")

        return html_parser

    def get_selector_value(self, html_parser, css_selector):
        return html_parser.select(css_selector)

    def search(self, start_date: datetime, end_date: datetime) -> list[list]:

        print(f"검색어: {self.keyword}")
        keyword = self.parse_keyword()
        date_range = self.get_date_range(start_date, end_date)

        # 변수 설정
        publish_date = []
        publisher_list = []
        news_title_list = []
        news_url_list = []
        news_description_list = []

        for date in date_range:

            print(f"진행일자: {date}")
            with requests.Session() as session:
                url_init = f"{self.NAVER_BASE_URL}where=news&{keyword}&sort=1&pd=3&ds={date}&de={date}&start=1"
                html_parser = self.parse_html(session, url_init)

            article_selector = (
                "div.group_news > ul.list_news > li div.news_area > a"
            )
            articles = self.get_selector_value(html_parser, article_selector)

            # 초기 카운터
            news_count_per_page = len(articles)
            page_num = 0

            publish_date.append([date for _ in range(news_count_per_page)])

            # 언론사 가져오기
            publisher_selector = "div.group_news > ul.list_news > li div.news_area > div.news_info > div.info_group > a.info.press"
            publishers = self.get_selector_value(
                html_parser, publisher_selector
            )
            publisher_list.append(
                [publisher.get_text(strip=True) for publisher in publishers]
            )

            # 뉴스기사 제목 가져오기
            news_title_list.append(
                [title.attrs["title"] for title in articles]
            )

            # 뉴스기사 URL 가져오기
            news_url_list.append([url.attrs["href"] for url in articles])

            # 뉴스 요약가져오기
            news_description_selector = "div.group_news > ul.list_news > li div.news_area > div.news_dsc > div.dsc_wrap > a"
            news_description = self.get_selector_value(
                html_parser, news_description_selector
            )
            news_description_list.append(
                [description.get_text() for description in news_description]
            )

            try:
                with requests.Session() as session:

                    while news_count_per_page > 0:

                        page_num += 1

                        reset_url = f"{self.NAVER_BASE_URL}where=news&{keyword}&sort=1&pd=3&ds={date}&de={date}&start={1+page_num*10}"
                        # print("new url: " + reset_url)
                        reset_html_parser = self.parse_html(session, reset_url)
                        reset_articles = self.get_selector_value(
                            reset_html_parser, article_selector
                        )

                        # 검색 결과 갯수 갱신
                        news_count_per_page = len(reset_articles)

                        if news_count_per_page:

                            publish_date.append(
                                [date for _ in range(news_count_per_page)]
                            )

                            # 언론사 가져오기
                            reset_publishers = self.get_selector_value(
                                reset_html_parser, publisher_selector
                            )
                            publisher_list.append(
                                [
                                    publisher.get_text(strip=True)
                                    for publisher in reset_publishers
                                ]
                            )

                            # 뉴스기사 제목 가져오기
                            news_title_list.append(
                                [
                                    title.attrs["title"]
                                    for title in reset_articles
                                ]
                            )

                            # 뉴스기사 URL 가져오기
                            news_url_list.append(
                                [url.attrs["href"] for url in reset_articles]
                            )

                            # 뉴스 요약가져오기
                            reset_news_description = self.get_selector_value(
                                reset_html_parser, news_description_selector
                            )
                            news_description_list.append(
                                [
                                    description.get_text()
                                    for description in reset_news_description
                                ]
                            )

                            # print("현재 페이지의 기사 갯수: " + str(news_count_per_page))

            except Exception as e:
                logger.exception(f"{self.search.__name__} --> {e}")
                raise

            time.sleep(0.5)
        return (
            publish_date,
            publisher_list,
            news_title_list,
            news_url_list,
            news_description_list,
        )

    def news_data_wrangler(
        self,
        publish_date: str,
        publisher_list: list[str],
        news_title_list: list[str],
        news_url_list: list[str],
        news_description_list,
    ) -> dict[list]:

        """kafka message형태로 전환"""

        news_collection_nested_list = {
            "publish_date": [
                datetime.strptime(row, "%Y.%m.%d")
                for lists in publish_date
                for row in lists
            ],
            "publisher": [row for lists in publisher_list for row in lists],
            "title": [row for lists in news_title_list for row in lists],
            "url": [row for lists in news_url_list for row in lists],
            "description": [
                row for lists in news_description_list for row in lists
            ],
        }

        news_collection_nested_list["keyword"] = [
            self.keyword
            for _ in range(len(news_collection_nested_list["title"]))
        ]

        # dict형태로 변환
        news_collection = [
            dict(zip(news_collection_nested_list.keys(), col))
            for col in zip(*news_collection_nested_list.values())
        ]

        return news_collection

    def run(self, start_date, end_date):

        (
            publish_date,
            publisher_list,
            news_title_list,
            news_url_list,
            news_description_list,
        ) = self.search(start_date, end_date)

        news_collection = self.news_data_wrangler(
            publish_date,
            publisher_list,
            news_title_list,
            news_url_list,
            news_description_list,
        )

        return news_collection


if __name__ == "__main__":

    keywords = ["인천세종병원", "심장내과", "흉부외과", "환자경험평가"]

    current_date = datetime.now(pytz.timezone("Asia/Seoul"))
    # 이틀전
    start_date = current_date - timedelta(days=2)
    # 일주일전
    # start_date = current_date - timedelta(weeks=1)
    # 한달전
    # start_date = (current_date - timedelta(days=current_date.day)).replace(day=1)

    end_date = current_date - timedelta(days=1)
    result = []
    # init
    for keyword in keywords:
        scraper = NaverNewsScraper(keyword)
        # result.append(scraper.run(start_date, end_date))
        news = scraper.run(start_date, end_date)
        for article in news:
            print(article)
            logger.info(article)
