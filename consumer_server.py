from kafka import KafkaConsumer
import time

TOPIC = "police.department.service.calls"


class Consumer(KafkaConsumer):
    '''
    A Kafka Consumer Class
    '''

    def __init__(self, topic):
        self.consumer = KafkaConsumer(
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            request_timeout_ms=1000,
            max_poll_records=5
        )
        self.consumer.subscribe(topics=topic)

    def consume(self):
        '''
        Consume data from a Kafka Topic
        :return:
        '''
        while True:
            for metadata, records in self.consumer.poll().items():
                if records:
                    for record in records:
                        print(record.value)
                        time.sleep(0.1)
                time.sleep(1.0)


if __name__ == "__main__":
    consumer = Consumer(TOPIC)
    consumer.consume()
