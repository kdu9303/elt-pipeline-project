# -*- coding: utf-8 -*-
import json
import requests
import logging
from typing import List, Dict
import datetime
from dateutil.rrule import rrule, MONTHLY
from dateutil.relativedelta import relativedelta
from airflow.models import Variable

# from kosis_statistics_config import secret

# 로그 기록용
logger = logging.getLogger()


class CensusDataScraper:
    """
    통계청 API로부터 시군구 계층의 인구수를 1달 단위로
    집계된 데이터를 조회한다.

    * Parameters *
    orgId: 기관 ID
    tblId: 통계표 ID
    objL1: 분류1(첫번째 분류코드)
    itmId: 항목
    loadGubun: 조회구분(1 시계열/2 횡단면)
    prdSe: 수록주기(M 월/Y 년)
    startPrdDe: 시작수록시점
    endPrdDe: 종료수록시점
    format:	결과 유형(json)

    """

    def __init__(self) -> None:

        self._API_KEY = Variable.get("KOSIS_API_SECRET")
        # self._API_KEY = secret.KOSIS_API_SECRET  # local

        self.method = "getList"
        self.orgId = "101"
        self.tblId = "DT_1B040A3"
        self.objL1 = "ALL"
        self.itmId = "T20"
        self.loadGubun = "2"
        self.prdSe = "M"
        self.format = "json"
        self.jsonVD = "Y"

        self.base_url = (
            "https://kosis.kr/openapi/Param/statisticsParameterData.do"
        )

    # 날짜 제너레이터 생성
    def get_date_range(
        self, startPrdDe: datetime.date, endPrdDe: datetime.date
    ) -> List[str]:
        """YYYYMM str 형태로 변환"""
        return [
            dt.strftime("%Y%m")
            for dt in rrule(MONTHLY, dtstart=startPrdDe, until=endPrdDe)
        ]

    def get_census_data(
        self, startPrdDe: datetime.date, endPrdDe: datetime.date
    ) -> List[Dict]:

        population = []

        try:
            for date in self.get_date_range(startPrdDe, endPrdDe):

                params = {
                    "apiKey": self._API_KEY,
                    "method": self.method,
                    "orgId": self.orgId,
                    "tblId": self.tblId,
                    "objL1": self.objL1,
                    "itmId": self.itmId,
                    "loadGubun": self.loadGubun,
                    "prdSe": self.prdSe,
                    "format": self.format,
                    "jsonVD": self.jsonVD,
                    "startPrdDe": date,
                    "endPrdDe": date,
                }

                r = requests.get(self.base_url, params=params)

                if r.status_code == 200:
                    result = json.loads(r.text)

                    if "err" in result:

                        # 데이터가 존재하지않으면 스킵한다
                        if "30" in result["err"]:
                            continue

                        else:
                            raise ValueError(result)

                    population.append(result)

                else:
                    r.close()
                    raise requests.HTTPError(
                        f"status code:<{r.status_code}> 발생. {r.reason}"
                    )

            population_flattened = [
                row for lists in population for row in lists
            ]

        except Exception as e:
            logger.exception(f"{self.get_census_data.__name__} --> {e}")
            raise

        return population_flattened


if __name__ == "__main__":

    current_month = datetime.date.today().replace(day=1)

    start_month = current_month + relativedelta(months=-2)

    population = CensusDataScraper()

    data = population.get_census_data(
        startPrdDe=start_month, endPrdDe=current_month
    )

    print(data)
