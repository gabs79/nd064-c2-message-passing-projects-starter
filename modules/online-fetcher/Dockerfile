FROM python:3.8
LABEL maintainer="Gabriel Enriquez"

WORKDIR /app

COPY requirements-runtime.txt ./
RUN pip install --no-cache-dir --requirement requirements-runtime.txt

COPY app/ ./

EXPOSE 5005
CMD [ "python", "server.py" ]