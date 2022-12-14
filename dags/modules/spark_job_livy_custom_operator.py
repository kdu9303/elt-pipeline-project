# -*- coding: utf-8 -*-
from datetime import datetime
from time import sleep
from typing import Any
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.apache.livy.hooks.livy import BatchState, LivyHook


class SparkSubmitOperator(BaseOperator):
    """
    Livy Rest Api를 통해 pyspark script를 batch형식의 Spark Job을 driver에 전달한다.
    기존 LivyOperator에서 Customize 한 내용
        - spark job 종료시 바로 batch 정보 삭제하는 함수 삭제 안하도록 수정
        - 단시간 안에 Retry할때 배치 이름 중복되는 현상 방지하는 로직 추가
        - Spark Config는 Spark Script안에서 정의하도록 Operator Parameter 제거

    * Parameters *
    file_name: Spark Cluster 내에 위치한 파일 위치
    livy_conn_id: Airflow Spark Driver Node Connection
    polling_interval: Livy Server에서 Spark Job의 Batch State 확인 시간
    delete_session: Spark Job 종료후 세션 정보 삭제 여부
    """

    def __init__(
        self,
        file_name: str,
        livy_conn_id: str,
        polling_interval: int = 15,
        delete_session: bool = False,
        **kwargs: Any,
    ) -> None:

        self.file_name = file_name
        self._livy_conn_id = livy_conn_id
        self._polling_interval = polling_interval
        self.delete_session = delete_session
        self._livy_hook: LivyHook = None
        super().__init__(**kwargs)

    def get_hook(self) -> LivyHook:
        self._livy_hook = LivyHook(livy_conn_id=self._livy_conn_id)
        return self._livy_hook

    def execute(self, context) -> Any:

        # 배치 이름이 겹치는 오류 방지
        post_fix = datetime.now().strftime("%Y%m%d-%H%M%S")

        batch_name = f"<{self.file_name}>-spark_job-{post_fix}"

        remote_file_path = f"/home/ec2-user/spark-data/{self.file_name}"

        livy_hook = self.get_hook()

        self._batch_id = livy_hook.post_batch(
            name=batch_name, file=f"file:{remote_file_path}"
        )

        # 배치 성공여부 확인 후 종료
        self.poll_for_termination(self._batch_id)

        # 세션 삭제
        if self.delete_session and self._batch_id is not None:
            livy_hook.delete_batch(self._batch_id)

    def poll_for_termination(self, batch_id) -> None:
        """spark job의 성공여부를 체크 후 세션을 종료한다"""
        livy_hook = self.get_hook()
        state = livy_hook.get_batch_state(batch_id)

        while state not in livy_hook.TERMINAL_STATES:
            self.log.debug(
                f"Batch with id {batch_id} is in state: {state.value}"
            )

            # status check(기본 15초)
            sleep(self._polling_interval)

            state = livy_hook.get_batch_state(batch_id)

        self.log.info(
            f"Batch with id {batch_id} terminated with state: {state.value}"
        )
        livy_hook.dump_batch_logs(batch_id)

        if state != BatchState.SUCCESS:
            raise AirflowException(f"Batch {batch_id} did not succeed")
