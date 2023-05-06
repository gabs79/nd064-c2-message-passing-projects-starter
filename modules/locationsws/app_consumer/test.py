from bson import dumps
from location_consumer import LocationConsumer

print('testing consumer')

loc_a = {
    "person_id":2,
    "latitude":38.97415890712725,
    "longitude":-77.52724542768364
}
loc_b = {
    "person_id":3,
    "latitude":38.9732414004479,
    "longitude":-77.52875819355961
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
