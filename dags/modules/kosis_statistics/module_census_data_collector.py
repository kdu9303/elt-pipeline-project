# -*- coding: utf-8 -*-
import json
import requests
import logging
from typing import List
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
    집계된 데이터를 불러온다.

    * Parameters *
    orgId: 기관 ID (필수)
    tblId: 통계표 ID (필수)
    objL1: 분류1(첫번째 분류코드) (필수)
    itmId: 항목	(필수)
    loadGubun: 조회구분(1 시계열/2 횡단면) (필수)
    prdSe: 수록주기(M 월/Y 년) (필수)
    startPrdDe: 시작수록시점
    endPrdDe: 종료수록시점
    format:	결과 유형(json) (필수)
    """

    def __init__(self) -> None:

        self._API_KEY = Variable.get("KOSIS_API_SECRET")

        self.orgId = "101"
        self.tblId = "DT_1B040A3"
        self.objL1 = "ALL"
        self.itmId = "T20"
        self.loadGubun = "2"
        self.prdSe = "M"
        self.format = "json"

        self.base_url = (
            "https://kosis.kr/openapi/Param/statisticsParameterData.do?"
            f"method=getList&apiKey={self._API_KEY}&"
            f"itmId={self.itmId}+&objL1={self.objL1}&"
            f"format={self.format}&jsonVD=Y&prdSe={self.prdSe}&"
            f"loadGubun={self.loadGubun}&orgId={self.orgId}&tblId={self.tblId}&"
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
    ) -> List[str]:

        population = []

        for date in self.get_date_range(startPrdDe, endPrdDe):
            url = self.base_url + f"startPrdDe={date}&endPrdDe={date}"

            try:
                r = requests.get(url)

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

            except Exception as e:
                logger.exception(e)
                raise

        return population


if __name__ == "__main__":

    current_month = datetime.date.today().replace(day=1)

    start_month = current_month + relativedelta(months=-6)

    population = CensusDataScraper()

    data = population.get_census_data(
        startPrdDe=start_month, endPrdDe=current_month
    )

    print(data)