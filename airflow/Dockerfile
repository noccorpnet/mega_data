FROM apache/airflow:2.9.2

USER root

RUN apt-get update

USER airflow

RUN pip install dbt-core dbt-postgres

USER root