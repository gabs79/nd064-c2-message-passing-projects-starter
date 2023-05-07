
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy import Column, Integer, String, DateTime, Text, BigInteger, Date, ForeignKey
from datetime import datetime
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from typing import Dict, List
from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape
from geoalchemy2.functions import ST_Point
from shapely.geometry.point import Point
from config import get_db_string
from sqlalchemy.orm import mapped_column
from sqlalchemy.ext.hybrid import hybrid_property
from datetime import datetime, timedelta
from sqlalchemy.sql import text

db_string = get_db_string()

engine = create_engine(db_string, echo=True)
Base = declarative_base()

class Person(Base):
    __tablename__ = "person"
    id = mapped_column(Integer(), primary_key=True)
    first_name = mapped_column(String(), nullable=False)
    last_name = mapped_column(String(), nullable=False)
    company_name = mapped_column(String(), nullable=False)

    def __repr__(self):
        _dict = {k:v for (k,v) in self.__dict__.items() if not k.startswith('_')}
        return f'{type(self).__name__}:{_dict}'

class LocationSentinel(Base):
    __tablename__ = "location_sentinel"
    last_location_id = mapped_column(Integer, nullable=False, default=0)

class Location(Base):
    __tablename__ = "location"
    __allow_unmapped__ = True

    id = mapped_column(BigInteger, primary_key=True)
    person_id = mapped_column(Integer, ForeignKey(Person.id), nullable=False)
    coordinate = mapped_column(Geometry("POINT"), nullable=False)
    creation_time = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    _wkt_shape: str = None

    @property
    def wkt_shape(self) -> str:
        # Persist binary form into readable text
        if not self._wkt_shape:
            point: Point = to_shape(self.coordinate)
            # normalize WKT returned by to_wkt() from shapely and ST_AsText() from DB
            self._wkt_shape = point.to_wkt().replace("POINT ", "ST_POINT")
        return self._wkt_shape

    @wkt_shape.setter
    def wkt_shape(self, v: str) -> None:
        self._wkt_shape = v

    def set_wkt_with_coords(self, lat: str, long: str) -> str:
        self._wkt_shape = f"ST_POINT({lat} {long})"
        return self._wkt_shape

    @hybrid_property
    def longitude(self) -> str:
        coord_text = self.wkt_shape
        return coord_text[coord_text.find(" ") + 1 : coord_text.find(")")]

    @hybrid_property
    def latitude(self) -> str:
        coord_text = self.wkt_shape
        return coord_text[coord_text.find("(") + 1 : coord_text.find(" ")]

    def __repr__(self):
        _dict = {k:v for (k,v) in self.__dict__.items() if not k.startswith('_')}
        return f'{type(self).__name__}:{_dict}'

class Connection(Base):
    __tablename__ = "connection"
    id = mapped_column(BigInteger, primary_key=True)
    from_person_id = mapped_column(Integer, ForeignKey(Person.id), nullable=False)
    to_person_id = mapped_column(Integer, ForeignKey(Person.id), nullable=False)
    longitude = mapped_column(String(), nullable=False)
    latitude = mapped_column(String(), nullable=False)
    exposed_time = Column(DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self):
        _dict = {k:v for (k,v) in self.__dict__.items() if not k.startswith('_')}
        return f'{type(self).__name__}:{_dict}'

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

def close_session():
    session.close()

class LocationSentinelDAO():
    @staticmethod
    def get_value() -> LocationSentinel:
        return session.query(LocationSentinel).one()
        
    @staticmethod
    def set_value(sentinel: LocationSentinel, new_value:int) -> None:
        sentinel.last_location_id = new_value
        session.commit()

class ConnectionDAO():
    @staticmethod
    def create(connection: Dict) -> Connection:
        new_connection = Connection()
        new_connection.from_person_id = connection["from_person_id"]
        new_connection.to_person_id = connection["to_person_id"]
        new_connection.longitude = connection["longitude"]
        new_connection.latitude = connection["latitude"]
        session.add(new_connection)
        session.commit()
        return new_connection
    
    @staticmethod
    def create_all(connections: List[Connection]) -> None:
        for connection in connections:
            session.add(connection)
        session.commit()

    @staticmethod
    def find_connections_for_person(person_id:int, person_locations: List[Location], meters:int=None, start_date:str=None, end_date:str=None) -> List[Connection]:
        found_connections:List[Connection] = []

        # Prepare arguments for queries
        data = []
        for location in person_locations:
            data.append(
                {
                    "person_id": person_id,
                    "longitude": location.longitude,
                    "latitude": location.latitude,
                    "meters": meters,
                    "start_date": start_date.strftime("%Y-%m-%d"),
                    "end_date": (end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
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
        for line in tuple(data):
            for (
                exposed_person_id,
                location_id,
                exposed_lat,
                exposed_long,
                exposed_time,
            ) in session.execute(query, **line):
                location = Location(
                    id=location_id,
                    person_id=exposed_person_id,
                    creation_time=exposed_time,
                )
                location.set_wkt_with_coords(exposed_lat, exposed_long)

                found_connection = Connection()
                found_connection.from_person_id = person_id
                found_connection.to_person_id = exposed_person_id
                found_connection.latitude = exposed_lat
                found_connection.longitude = exposed_long
                found_connection.exposed_time = exposed_time
                found_connections.append(found_connection)
        return found_connections

class PersonDAO():
    @staticmethod
    def get_person(person_id: int) -> Person:
        return session.query(Person).get(person_id)

    @staticmethod
    def retrieve_all() -> List[Person]:
        return session.query(Person).all()

    @staticmethod
    def add_person(person: Dict) -> Person:
        new_person = Person(
            first_name = person["first_name"],
            last_name = person["last_name"],
            company_name = person["company_name"]
        )
        session.add(new_person)
        session.commit()
        return new_person

class LocationDAO():
    @staticmethod
    def get_after_sentinel(sentinel_value:int) -> List[Location]:
        return session.query(Location).filter(
            Location.id > sentinel_value
        ).all()

    @staticmethod
    def retrieve(location_id) -> Location:
        location, coord_text = (
            session.query(Location, Location.coordinate.ST_AsText())
            .filter(Location.id == location_id)
            .one()
        )

        # Rely on database to return text form of point to reduce overhead of conversion in app code
        location.wkt_shape = coord_text
        return location

    @staticmethod
    def create(location: Dict) -> Location:
        new_location = Location()
        new_location.person_id = location["person_id"]
        #rely on DB
        #new_location.creation_time = location["creation_time"]
        new_location.coordinate = ST_Point(location["latitude"], location["longitude"])
        session.add(new_location)
        session.commit()
        return new_location

#session.add_all([p1, p2, p3])