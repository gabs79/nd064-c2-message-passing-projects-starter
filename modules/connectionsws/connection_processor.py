from connectiondao import (
    ConnectionDAO, LocationDAO, Connection, LocationSentinelDAO, Location,
    LocationSentinel, PersonDAO, Person)
from typing import Dict, List
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("connection-processor")

 #todo: see how to propagete from UI, but this is among many other questions to ask
 # about this app before finishing a suitable design
METERS = 5
START_DATE='2020-01-01'
END_DATE='2020-12-30'

class ConnectionProcessor():
    def process(self):
        last_processed: LocationSentinel = LocationSentinelDAO.get_value()
        logging.warn(f'>>>>>Last processed:{last_processed.last_location_id}')
        locations: List[Location] = LocationDAO.get_after_sentinel(last_processed.last_location_id)
        if locations:
            logging.warn(f'>>>>>Found {len(locations)} new locations to process.')
            new_connections = self._process_connections(locations)
            if new_connections:
                ConnectionDAO.create_all(new_connections)

            #update new sentinel value
            new_sentinel = self._get_new_sentinel(locations)
            LocationSentinelDAO.set_value(last_processed, new_sentinel)
        else:
            logger.warn(f'Nothing to process. Current sentinel value:{last_processed.last_location_id}')

    def _process_connections(self, locations: List[Location]) -> List[Connection]:
        new_connections: List[Connection] = []

        # Cache all users in memory for quick lookup
        person_ids = [person.id for person in PersonDAO.retrieve_all()]

        for _person_id in person_ids:
            found_connections = ConnectionDAO.find_connections_for_person(
                person_id=_person_id,
                person_locations=self._filter_by_person(_person_id, locations),
                meters=METERS,
                start_date=START_DATE,
                end_date=END_DATE
                )
            new_connections.extend(found_connections)

        return new_connections
    
    def _filter_by_person(self, person_id:int, all_locations: List[Location]) -> List[Connection]:
        return [location for location in all_locations if location.person_id == person_id]

    def _get_new_sentinel(self, locations: List[Location]) -> int:
        return max(locations, key=lambda x: x.id)

if __name__ == "__main__":
    logging.warn(f'Started processing connections')
    processor = ConnectionProcessor()
    processor.process()