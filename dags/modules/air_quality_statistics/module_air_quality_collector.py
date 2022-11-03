# -*- coding: utf-8 -*-
import json
import requests
import logging
from typing import Dict

# from airflow.models import Variable
from air_quality_statistics_config import secret

# 로그 기록용
logger = logging.getLogger()


class AirQualityDataScraper:
    """
    공공데이터 API로부터 시도의 각 시군구의 대기 측정소로부터
    한시간 단위로 실시간 평균 대기 정보(SO2, CO, O3, NO2, PM10, PM25)를
    조회한다.

    * Parameters *
    serviceKey: 인증키(URL Encode)
    returnType: 데이터표출방식(xml/json)
    numOfRows: 한 페이지 결과 수
    pageNo: 페이지 번호
    sidoName: 시도명(서울,경기,인천, etc)
    searchCondition: 데이터 기간(HOUR/DAILY)
    """

    def __init__(self) -> None:

        # self._API_KEY = Variable.get("OPEN_API_SECRET")
        self._API_KEY = secret.OPEN_API_SECRET  # local

        self.returnType = "json"
        self.numOfRows = "400"
        self.searchCondition = "DAILY"

        self.base_url = "http://apis.data.go.kr/B552584/ArpltnStatsSvc/getCtprvnMesureSidoLIst"

    def get_air_quality_data(self, sidoName: str) -> Dict:

        params = {
            "serviceKey": self._API_KEY,
            "returnType": self.returnType,
            "numOfRows": self.numOfRows,
            "sidoName": sidoName,
            "searchCondition": self.searchCondition,
        }

        try:
            r = requests.get(self.base_url, params=params)

            if r.status_code == 200:
                result = json.loads(r.text)

                if result["response"]["header"].get("resultCode") != "00":
                    err_code = result["response"]["header"].get("resultCode")
                    raise requests.HTTPError(
                        f"error code:<{err_code}> 발생. OpenAPI 에러코드를 참조하세요."
                    )

                data = result["response"]["body"]["items"]

            else:
                r.close()
                raise requests.HTTPError(
                    f"status code:<{r.status_code}> 발생. {r.reason}"
                )

        except Exception as e:
            logger.exception(f"{self.get_air_quality_data.__name__} --> {e}")
            raise

        return data


if __name__ == "__main__":

    air_quality_scraper = AirQualityDataScraper()

    city_list = ["경기"]

    for city in city_list:

        data = air_quality_scraper.get_air_quality_data(sidoName=city)

    print(data)
