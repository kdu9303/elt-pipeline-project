# MWAA airflow버전
FROM apache/airflow:2.2.2-python3.7
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

  # timezone setting
ENV TZ=Asia/Seoul
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone


# instanct client(오라클DB)
# TNS_ADMIN 변수(instanct client 경로) 체크할 것
# sqlnet.ora DIRECTORY 확인
# docker 컨테이너 안에 sqlnet.ora 경로 변경 필수
# COPY dependencies/instantclient_21_3 ./dependencies/instantclient_21_3
# ENV LD_LIBRARY_PATH=/opt/airflow/dependencies/instantclient_21_3
# ENV PATH "$PATH:${LD_LIBRARY_PATH}"


ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/dags/"

USER airflow

COPY requirements.txt /requirements.txt

RUN /usr/local/bin/python -m pip install --upgrade pip \
  && pip install apache-airflow-providers-amazon \
  && pip install --no-cache-dir --user -r /requirements.txt
