from connectiondao import ConnectionDAO, LocationDAO, Connection, LocationSentinelDAO, Location, LocationSentinel
from typing import Dict, List
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("connection-processor")


class ConnectionProcessor():
    def process(self):
        last_processed: LocationSentinel = LocationSentinelDAO.get_value()
        locations: List[Location] = LocationDAO.get_after_sentinel(last_processed.last_location_id)
        if locations:
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
        person_map: Dict[str, Person] = {person.id: person for person in PersonService.retrieve_all()}

        for person_id in person_map.keys():
            found_connections = _found_connections_for_person(person_id)
            new_connections.extend(found_connections)

        return new_connections
    
    def _found_connections_for_person(self, person_id:int) -> List[Connection]:
        found_connections = []
        return found_connections

    def _get_new_sentinel(self, locations: List[Location]) -> int:
        return max(locations, key=lambda x: x.id)

def start():
    pass

if __name__ == "__main__":
    start()