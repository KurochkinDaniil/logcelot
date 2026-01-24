FROM apache/airflow:2.8.1-python3.11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY infrastructure/airflow/requirements-airflow.txt /requirements-airflow.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements-airflow.txt

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/dags:/opt/airflow"
