FROM apache/airflow:2.10.5
# temporary switch to root user to install applications
USER root 

RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/airflow
ENV PYTHONPATH=/opt/airflow
COPY . .

USER airflow
RUN pip install --no-cache-dir -r requirements.txt