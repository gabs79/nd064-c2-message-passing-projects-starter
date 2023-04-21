FROM python:3.8
LABEL maintainer="Gabriel Enriquez"

WORKDIR /app

COPY requirements-build.txt /app
RUN pip install --no-cache-dir --requirement requirements-build.txt

COPY server.py \
    person_pb2_grpc.py \
    person_pb2.py \
    ./

EXPOSE 5005
CMD [ "python", "server.py" ]