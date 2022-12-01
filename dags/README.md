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
- spark_job_livy_custom_operator.py

