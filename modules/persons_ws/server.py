import time
from concurrent import futures

import grpc
import person_pb2
import person_pb2_grpc
import logging


class PersonServicer(person_pb2_grpc.PersonServiceServicer):
    def Create(self, request, context):

        request_value = {
            "first_name": request.first_name,
            "last_name": request.last_name,
            "company_name": request.company_name
        }
        print(f'>>>>>> TODO add into db: {request_value}')
        logging.warning(f'>>>>>> TODO add into db: {request_value}')

        return person_pb2.PersonMessage(**request_value)


# Initialize gRPC server
server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
person_pb2_grpc.add_PersonServiceServicer_to_server(PersonServicer(), server)


print("Server starting on port 5005...")
server.add_insecure_port("[::]:5005")
server.start()
# Keep thread alive
try:
    while True:
        time.sleep(86400)
except KeyboardInterrupt:
    server.stop(0)