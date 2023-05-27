from typing import Optional
from bson import dumps, loads
from kafka import KafkaConsumer
import logging
import time

class LocationConsumer():
    def __init__(self, kafka_consumer=Optional[KafkaConsumer], location_dao=None) -> None:
        self.kafka_consumer = kafka_consumer
        self.location_dao = location_dao

    def consume(self) -> None:
        for message in self.kafka_consumer:
            loc = loads(message)
            logging.warning(f'>>>>>> Got kafka location: {loc}')
            self.location_dao.create(loc)

def create_kafka_consumer(topic_name:str):
    return KafkaConsumer(topic_name)

TOPIC_NAME = 'items'
def main():
    from locationdao import LocationDAO
    location_consumer = LocationConsumer(kafka_consumer=create_kafka_consumer(TOPIC_NAME), location_dao=LocationDAO)
    while True:
        location_consumer.consume()

if __name__ == "__main__":
    main()