import grpc
import person_pb2
import person_pb2_grpc

print("Sending sample payload...")

channel = grpc.insecure_channel("localhost:5005")
stub = person_pb2_grpc.PersonServiceStub(channel)

# Update this with desired payload
person = person_pb2.PersonMessage(
    first_name="a first name",
    last_name="a last name",
    company_name="the company"
)


response = stub.Create(person)