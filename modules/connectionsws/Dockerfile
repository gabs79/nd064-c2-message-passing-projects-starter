FROM python:3.9.16
LABEL maintainer="Gabriel Enriquez"

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir --requirement requirements.txt

COPY app/ ./

CMD [ "python", "connection_processor.py" ]