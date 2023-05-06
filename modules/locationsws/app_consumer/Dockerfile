FROM python:3.7-alpine

WORKDIR .

COPY requirements-consumer.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .
CMD ["python", "location_consumer.py"]