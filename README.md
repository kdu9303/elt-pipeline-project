![](https://img.shields.io/badge/python-v3.7-blue.svg) ![](https://img.shields.io/badge/kafka-v2.7.2-red.svg) ![](https://img.shields.io/badge/spark-v3.2.2-orange.svg) ![](https://img.shields.io/badge/airflow-v2.2.2-green.svg)

# ELT Pipeline Project
**Kafka**의 Pub/Sub 시스템을 중심으로 **S3 저장소**의 Bronze bucket에 Raw data를 저장하고 **Spark**로 Transform 된 Data를 다시 Silver bucket에 저장하는 Extract Load Transform 구조입니다.


## 1. 도입 목적
- Scale in/out이 용이한 분산 시스템 환경 구축
- Data loss 최소화
- Architecture 확장시 단순한 구조 유지
- Object Storage에서의 데이터 정합성 유지

## 2. Architecture
<p align="center"><img src="https://github.com/kdu9303/elt-pipeline-project/blob/main/jpg/ELT-pipeline.jpg" width="740" height="400"/></p>


## 3. 설명
### 3-1. 분산 시스템 환경 구축
- 트래픽에 따라 Kafka와 Spark의 Cluster를 조절하여 비용 최소화

### 3-2. Data loss 최소화
- Kafka는 Memmory 기반의 메세징 시스템과는 다르게 이미 소비된 데이터도 보존하므로 재사용 가능
- 분산 서버 기반으로 서버 장애시에도 고가용성 보장

### 3-3. Architecture 단순화
- Data Application은 메세지 전송 역할로만 제한하여 Application 구조 단순화 및 Scheduler 서버 자원 사용 최소화
- Kafka Connect로 S3, DB등 외부 시스템 연동 Pipeline 구성 단순화

### 3-4. Object Storage에서의 데이터 정합성 유지
- Storage Layer(Delta Lake)를 도입함으로써 저장소를 RDBMS와 같이 Update, Merge 작업 가능

## 4. Pipeline 흐름도

### 4-1. Airflow
Dag에서 불러오는 Source 파일은 dags/modules에서 공통 Module과 Dag 주제별 폴더에서 관리되고 있습니다.
Git workflow를 통해 Dag 파일의 import error test와 code formatting을 거쳐 Airflow EC2 Instance로 Sync 됩니다.

<p align="center"><img src="https://github.com/kdu9303/elt-pipeline-project/blob/main/jpg/airflow-task-flow.jpg" width="740" height="80"/></p>

[Dag Task 구성 바로가기](https://github.com/kdu9303/elt-pipeline-project/tree/main/dags)

### 4-2. Logstash
Spark cluster에 설치된 Filebeat에서 실시간으로 Spark log를 Kafka Broker로 전송하고 Logstash를 통해 Bronze bucket으로 적재됩니다.

## 5. 인프라 구성

- Kafka와 Spark Cluster는 각각 3개의 EC2 Instance로 구성
- Cluster의 각 Node는 private ip로 서로 통신
- Kafka의 End point는 외부 접속을 위해 public ip로 구성
- Airflow와 Kafka Connect의 구성요소는 별도의 EC2 Instance에서 컨테이너 환경으로 구성

## 6. 구현

<details>
<summary><strong>AWS 구성 화면</strong></summary>
<h4>EC2 Instance 구성</h4>
<p align="center"><img src="https://github.com/kdu9303/elt-pipeline-project/blob/main/jpg/example_ec2_instance.jpg" width="740" height="240"/></p>

<h4>Delta lake 구조의 Silver bucket</h4>
<p align="center"><img src="https://github.com/kdu9303/elt-pipeline-project/blob/main/jpg/example_s3_deltalake.jpg" width="740" height="170"/></p>

<h4>Athena Query 결과</h4>
<p align="center"><img src="https://github.com/kdu9303/elt-pipeline-project/blob/main/jpg/example_athena_result.jpg" width="740" height="200"/></p>
</details>

---
