# -*- coding: utf-8 -*-
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException

conf = {
    "bootstrap.servers": "43.201.13.181:9092, 43.200.251.62:9092, 52.78.78.140:9092",
    "group.id": "from_console",
    "enable.auto.commit": True,
    "auto.offset.reset": "earliest",
}

consumer = Consumer(conf)
topics = ["spark-application-log"]
running = True


def consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        msg_count = 0
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write(
                        "%% %s [%d] reached end at offset %d\n"
                        % (msg.topic(), msg.partition(), msg.offset())
                    )
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                data = msg.value().decode("utf-8")
                print(data)
                msg_count += 1
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


if __name__ == "__main__":
    consume_loop(consumer, topics)
