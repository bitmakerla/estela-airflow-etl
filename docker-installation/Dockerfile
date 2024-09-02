FROM apache/airflow:latest

USER root

COPY --chown=airflow:root ./dags/ /usr/src/app/dags/
COPY --chown=airflow:root ./dags/ /opt/airflow/dags/
USER airflow
WORKDIR /usr/src/app
COPY requirements.txt .
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt
