from typing import Optional
from kafka import KafkaConsumer
from bson import dumps, loads

def create_kafka_consumer(topic_name:str):
    return KafkaConsumer(topic_name)

class LocationConsumer():
    def __init__(self, kafka_consumer=Optional[KafkaConsumer], location_dao=None) -> None:
        self.kafka_consumer = kafka_consumer
        self.location_dao = location_dao

    def consume(self) -> None:
        for message in self.kafka_consumer:
            self.location_dao.create(loads(message))

def start():
    location_consumer = LocationConsumer()
    while True:
        location_consumer.consume()

if __name__ == "__main__":
    print('testing consumer')
    
    loc_a = {
        "person_id":2,
        "coordinate":"coordinatexy"
    }

    loc_b = {
        "person_id":3,
        "coordinate":"coordinatexyz"
    }
    
    locations = (
        dumps(loc_a),
        dumps(loc_b)
    )
    mocked_kafka_consumer = iter(locations)

    class MockedLocationDao():
        def __init__(self) -> None:
            self.locations=[]

        def create(self, loc):
            self.locations.append(loc)

    mocked_dao = MockedLocationDao()
    test_consumer = LocationConsumer(kafka_consumer=mocked_kafka_consumer, location_dao=mocked_dao)
    test_consumer.consume()

    total_dao_sinked_locations = len(mocked_dao.locations)
    print(f'msgs consumed total:{total_dao_sinked_locations}; locations:{mocked_dao.locations}')
    if total_dao_sinked_locations != 2:
        raise Exception(f"should have sent {len(locations)} locations to DAO")