from typing import Optional
from bson import dumps, loads
from kafka import KafkaConsumer
import logging
import time
import os

class LocationConsumer():
    def __init__(self, kafka_consumer=Optional[KafkaConsumer], location_dao=None) -> None:
        self.kafka_consumer = kafka_consumer
        self.location_dao = location_dao

    def consume(self) -> None:
        for message in self.kafka_consumer:
            loc = loads(message)
            logging.warning(f'>>>>>> Got kafka location: {loc}')
            self.location_dao.create(loc)

def create_kafka_consumer(topic_name:str, **kwargs):
    return KafkaConsumer(topic_name, kwargs)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_SERVER = os.getenv("KAFKA_SERVER")
KAFKA_PORT = os.getenv("KAFKA_PORT")
def main():
    from locationdao import LocationDAO
    location_consumer = LocationConsumer(kafka_consumer=create_kafka_consumer(KAFKA_TOPIC, bootstrap_servers=f'{KAFKA_SERVER}:{KAFKA_PORT}'), location_dao=LocationDAO)
    while True:
        location_consumer.consume()

if __name__ == "__main__":
    main()