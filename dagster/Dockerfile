FROM python:3.10-slim

WORKDIR /app

RUN pip install dagster

RUN cd dbt && dbt parse

EXPOSE 4004 8000

CMD ["dagster", "api", "grpc", "--python-file", "/app/repository.py", "--attribute", "datamart", "--host", "0.0.0.0", "--port", "4004"]