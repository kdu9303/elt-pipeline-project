# -*- coding: utf-8 -*-
import json
import requests
import logging
from typing import List
import datetime
from dateutil.rrule import rrule, MONTHLY
from dateutil.relativedelta import relativedelta
from kosis_statistics_config import secret

# 로그 기록용
logger = logging.getLogger()


class CensusDataScraper:
    """
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

    def __init__(self, startPrdDe: datetime, endPrdDe: datetime) -> None:

        self._API_KEY = secret.KOSIS_API_SECRET

        self.orgId = "101"
        self.tblId = "DT_1B040A3"
        self.objL1 = "ALL"
        self.itmId = "T20"
        self.loadGubun = "2"
        self.prdSe = "M"
        self.format = "json"

        self.startPrdDe = startPrdDe
        self.endPrdDe = endPrdDe

        self.base_url = (
            "https://kosis.kr/openapi/Param/statisticsParameterData.do?"
            f"method=getList&apiKey={self._API_KEY}&"
            f"itmId={self.itmId}+&objL1={self.objL1}&"
            f"format={self.format}&jsonVD=Y&prdSe={self.prdSe}&"
            f"loadGubun={self.loadGubun}&orgId={self.orgId}&tblId={self.tblId}&"
        )

    # 날짜 제너레이터 생성
    def get_date_range(self) -> List[str]:
        """YYYYMM str 형태로 변환"""
        return [
            dt.strftime("%Y%m")
            for dt in rrule(
                MONTHLY, dtstart=self.startPrdDe, until=self.endPrdDe
            )
        ]

    def get_census_data(self) -> List[str]:

        population = []

        for date in self.get_date_range():
            url = self.base_url + f"startPrdDe={date}&endPrdDe={date}"

            try:
                r = requests.get(url)

                if r.status_code == 200:

                    if "err" in eval(r.text):

                        # 데이터가 존재하지않으면 스킵한다
                        if eval(r.text)["err"] == "30":
                            continue
                        else:
                            raise ValueError(eval(r.text))

                    population.append(json.loads(r.text))

                else:
                    r.close()
            except Exception as e:
                logger.exception(e)
                raise

        return population


if __name__ == "__main__":

    current_month = datetime.date.today().replace(day=1)

    start_month = current_month + relativedelta(months=-2)

    population = CensusDataScraper(
        startPrdDe=start_month, endPrdDe=current_month
    )

    data = population.get_census_data()

    print(data)
