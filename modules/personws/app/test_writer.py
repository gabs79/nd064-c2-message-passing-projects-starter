import grpc
import person_pb2
import person_pb2_grpc

print("Sending sample payload...")

channel = grpc.insecure_channel("localhost:5005")
stub = person_pb2_grpc.PersonServiceStub(channel)

from dao.personsdao import PersonDAO, LocationDAO, close_session, Person
persons = PersonDAO.retrieve_all()

print(f'>>>>>>>>>>>>>>>>> before db persons[{len(persons)}]; persons:{persons}')

# Update this with desired payload
person = person_pb2.PersonMessage(
    first_name="a first name",
    last_name="a last name",
    company_name="the company"
)

response = stub.Create(person)

persons = PersonDAO.retrieve_all()
print(f'>>>>>>>>>>>>>>>>>>> aft db persons[{len(persons)}]; persons:{persons}')