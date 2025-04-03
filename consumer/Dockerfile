FROM python:3.9-slim

WORKDIR /app

RUN pip install kafka-python

COPY consumer.py .

CMD ["python", "consumer.py"]