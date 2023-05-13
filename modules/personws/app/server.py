import time
from concurrent import futures

import grpc
import person_pb2
import person_pb2_grpc
import logging

from dao.personsdao import PersonDAO, close_session
class PersonServicer(person_pb2_grpc.PersonServiceServicer):
    def Create(self, request, context):
        request_value = {
            "first_name": request.first_name,
            "last_name": request.last_name,
            "company_name": request.company_name
        }
        added_person = PersonDAO.add_person(request_value)
        logging.warning(f'>>>>>> Added person to DB with id:{added_person.id}')
        request_value['id'] = added_person.id
        return person_pb2.PersonMessage(**request_value)

# Initialize gRPC server
server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
person_pb2_grpc.add_PersonServiceServicer_to_server(PersonServicer(), server)

logging.warning(f"Server starting on port 5005")
server.add_insecure_port("[::]:5005")
server.start()
# Keep thread alive
try:
    while True:
        time.sleep(86400)
except KeyboardInterrupt:
    logging.warning(f'>>>>>> Stopping GRPC PersonServicer port 5005')
    server.stop(0)
    logging.warning(f'>>>>>> Closing DB session')
    close_session()
    logging.warning(f'>>>>>> All done!!')