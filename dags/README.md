## 1. Dag 구성
Dag 파일에서 실행 모듈과 Kafka Producer 모듈을 호출하여 실행되는 구조입니다.

### 1-1. 공통 Task 구성
- Producer
    - 데이터 호출 Module을 실행시킨 후 Kafka broker로 전송
- S3 Sensor
    - Kafka sink connector에서 S3 bronze bucket에 데이터 적재 유무 감지
- Script File Transter
    - Spark Script를 Spark Driver Node로 전송
- File Sensor
    - Spark Driver Node에 Script File 존재 유무 감지
- Run Spark Job
    - Livy REST API를 통해 Spark Cluster로 Spark Job을 제출

### 1-2. Module 구성

#### 공통 Module

- producer.py
    - Kafka Schema Registry에 사용되는 Avro Schema와의 호환성으로 AvroProcuder 사용

- spark_job_livy_custom_operator.py
    - Airflow 환경에 Spark 설치가 필요한 SparkSubmitOperator 대신 Spark Cluster 원격 실행이 가능한 Livy Operator 사용

#### 주제별 Module
- 데이터를 호출하는 module 파일과 데이터 변환을 담당하는 Spark script 파일로 구성
- avro_schema 폴더에 정의된 데이터 Schema파일을 Dag에서 호출 

## 2. Dag 화면 예제

<details>
<summary><strong>화면 Example</strong></summary>
<h4>Airflow Main 화면</h4>
<p align="center"><img src="https://github.com/kdu9303/elt-pipeline-project/blob/main/jpg/example_airflow_1.jpg" width="740" height="200"/></p>


<h4>Task flow</h4>
<p align="center"><img src="https://github.com/kdu9303/elt-pipeline-project/blob/main/jpg/example_airflow_2.jpg" width="740" height="220"/></p>
</details>

---
