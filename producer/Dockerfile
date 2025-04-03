FROM python:3.9-slim

WORKDIR /app

RUN pip install websocket-client kafka-python

COPY producer.py .

CMD ["python", "producer.py"]