
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
from dao.config import get_db_string
from sqlalchemy.orm import mapped_column
from sqlalchemy.ext.hybrid import hybrid_property
import logging

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

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

def close_session():
    session.close()

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