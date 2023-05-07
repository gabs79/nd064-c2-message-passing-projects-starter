from connectiondao import (
    ConnectionDAO, LocationDAO, Connection, LocationSentinelDAO, Location,
    LocationSentinel, PersonDAO, Person)
from typing import Dict, List
import logging
from datetime import datetime, timedelta
from sqlalchemy.sql import text

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
        person_ids = [person.id for person in PersonDAO.retrieve_all()]

        for person_id in person_ids:
            person_locations = self._filter_by_person(person_id, locations)
            found_connections = self._find_connections_for_person(person_id, person_locations)
            new_connections.extend(found_connections)

        return new_connections
    
    def _filter_by_person(self, person_id:int, all_locations: List[Location]) -> List[Connection]:
        return [location for location in all_locations if location.person_id == person_id]

    def _find_connections_for_person(self, person_id:int, person_locations: List[Location]) -> List[Connection]:
        found_connections = []

        # Prepare arguments for queries
        data = []
        for location in person_locations:
            data.append(
                {
                    "person_id": person_id,
                    "longitude": location.longitude,
                    "latitude": location.latitude,
                    "meters": METERS,
                    "start_date": START_DATE.strftime("%Y-%m-%d"),
                    "end_date": (END_DATE + timedelta(days=1)).strftime("%Y-%m-%d"),
                }
            )

        query = text(
            """
        SELECT  person_id, id, ST_X(coordinate), ST_Y(coordinate), creation_time
        FROM    location
        WHERE   ST_DWithin(coordinate::geography,ST_SetSRID(ST_MakePoint(:latitude,:longitude),4326)::geography, :meters)
        AND     person_id != :person_id
        AND     TO_DATE(:start_date, 'YYYY-MM-DD') <= creation_time
        AND     TO_DATE(:end_date, 'YYYY-MM-DD') > creation_time;
        """
        )
        result: List[Connection] = []
        for line in tuple(data):
            for (
                exposed_person_id,
                location_id,
                exposed_lat,
                exposed_long,
                exposed_time,
            ) in db.engine.execute(query, **line):
                location = Location(
                    id=location_id,
                    person_id=exposed_person_id,
                    creation_time=exposed_time,
                )
                location.set_wkt_with_coords(exposed_lat, exposed_long)

                result.append(
                    Connection(
                        person=person_map[exposed_person_id], location=location,
                    )
                )


        return found_connections

    def _get_new_sentinel(self, locations: List[Location]) -> int:
        return max(locations, key=lambda x: x.id)

def start():
    pass

if __name__ == "__main__":
    start()