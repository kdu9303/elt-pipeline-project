# -*- coding: utf-8 -*-
import uuid
import traceback
import logging
from kafka.errors import KafkaError
from airflow.exceptions import AirflowException
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer


class MessageProducer:
    """
    Avro Scehma 등록과 Kafka Broker로 Dictonary 형태의 데이터 전송을 수행한다.

    * Parameters *
    topic: Kafka Topic
    key_schema_path: 정의된 Avro Scehma 경로
    value_schema_path: 정의된 Avro Scehma 경로
    """

    def __init__(
        self, topic: str, key_schema_path: str, value_schema_path: str
    ) -> None:

        self.topic = topic
        self.key_schema = avro.load(key_schema_path)
        self.value_schema = avro.load(value_schema_path)

        self.producer_conf = {
            "bootstrap.servers": "43.201.13.181:9092,43.200.251.62:9092,52.78.78.140:9092",
            "schema.registry.url": "http://43.200.243.204:8081/",
            "acks": "all",
            "enable.idempotence": "True",  # Kafka Broker로의 데이터 중복 전송 방지
        }

        self.producer = AvroProducer(
            self.producer_conf,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
        )

    def produce(self, data: dict = None) -> None:

        try:
            message_key = {"message_key": str(uuid.uuid4())}

            self.producer.produce(
                key_schema=self.key_schema,
                value_schema=self.value_schema,
                topic=self.topic,
                key=message_key,
                value=data,
            )

            # logging.info(f"key: {message_key}, message: {data}")

            self.producer.flush()

        except KafkaError:
            raise AirflowException(traceback.format_exc())

        except TypeError:
            raise AirflowException(
                "Producer data type must be dictionary type"
            )

        except AttributeError:
            raise AirflowException(
                "Producer data type must be dictionary type"
            )

        except Exception:
            raise AirflowException(traceback.format_exc())


def send_example():

    # path는 docker 컨테이너 내부 경로로 설정
    key_schema_path = (
        "/opt/airflow/dags/modules/avro_schema/test_schema_key.avsc"
    )
    value_schema_path = (
        "/opt/airflow/dags/modules/avro_schema/test_schema_value.avsc"
    )
    topic = "test"

    message_producer = MessageProducer(
        topic, key_schema_path, value_schema_path
    )

    data = {"name": "abc2", "email": "abc2@example.com"}

    logging.info("<<< producer start >>>")
    message_producer.produce(data)
    logging.info("message sent successfully...")


if __name__ == "__main__":
    send_example()
