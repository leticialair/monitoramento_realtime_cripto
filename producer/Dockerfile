FROM python:3.9-slim

WORKDIR /app

RUN pip install kafka-python websocket-client

CMD ["python", "producer.py"]