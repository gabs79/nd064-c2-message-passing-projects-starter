import grpc
import app.udaconnect.person_pb2
import app.udaconnect.person_pb2_grpc
from typing import Dict

def send_person(person_dict: Dict):
    #todo add WS host as ENV_VARIABLE
    with grpc.insecure_channel("udaconnect-person:5005") as channel:
        stub = app.udaconnect.person_pb2_grpc.PersonServiceStub(channel)

        # Update this with desired payload
        person = app.udaconnect.person_pb2.PersonMessage(
            first_name = person_dict['first_name'],
            last_name = person_dict['last_name'],
            company_name = person_dict['company_name']
        )
        return stub.Create(person)

if __name__  == "__main__":
    print('testing person producer (after starting personws/app/server consumer)...')
    person = {
        "first_name": "Gabe",
        "last_name": "En",
        "company_name": "LM"
    }
    added_person = send_person(person)
    print(f'>>> Sent person:{person}')
    print(f'>>> Returned person:{added_person}')