FROM python:3.8
LABEL maintainer="Gabriel Enriquez"

WORKDIR .

COPY requirements-consumer.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["python", "location_consumer.py"]