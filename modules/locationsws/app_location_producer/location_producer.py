from kafka import KafkaProducer
from bson import dumps, loads

def create_kafka_producer(kafka_server:str) -> KafkaProducer:
    return KafkaProducer(bootstrap_servers=kafka_server)

class LocationProducer():
    def __init__(self, topic_name:str=None, kafka_producer:KafkaProducer=None) -> None:
        self.topic_name = topic_name
        self.producer = kafka_producer

    def send(self, obj) -> None:
        self.producer.send(self.topic_name, dumps(obj))
        self.producer.flush()

if __name__ == "__main__":
    print('sending kafka msg')

    class TestProducer():
        def send(self, topic_name, obj):
            self.topic_name = topic_name
            self.obj = obj
    
        def flush(self):
            self.flush = True

    TOPIC_NAME = 'items'
    KAFKA_SERVER = 'localhost:9092'
    kafka_mock = TestProducer()
    producer = LocationProducer(topic_name=TOPIC_NAME, kafka_producer=kafka_mock)
   
    loc_a = {
        "person_id":2,
        "latitude":38.97415890712725,
        "longitude":-77.52724542768364
    }
    producer.send(loc_a)

    print(f'Sent to kafka: topic={kafka_mock.topic_name}; flushed={kafka_mock.flush}; obj={kafka_mock.obj}')
    print(f'decoded obj:{loads(kafka_mock.obj)}')
    if kafka_mock.obj is None:
        raise Exception(f'kafka_mock.obj should have the sent obj')