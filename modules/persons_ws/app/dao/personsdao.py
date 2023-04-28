
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy import Column, Integer, String, DateTime, Text
from datetime import datetime
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from typing import Dict, List
from dao.config import get_db_string

db_string = get_db_string()

engine = create_engine(db_string, echo=True)
Base = declarative_base()

class Person(Base):
    __tablename__ = "person"
    id = Column(Integer(), primary_key=True)
    first_name = Column(String(), nullable=False)
    last_name = Column(String(), nullable=False)
    company_name = Column(String(), nullable=False)

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
#session.add_all([p1, p2, p3])