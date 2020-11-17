FROM python:3.7.9-slim-buster

ENV DEBIAN_FRONTEND="noninteractive"
ENV AIRFLOW__CORE__LOAD_EXAMPLES="False"
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION="False"

RUN apt-get update && \
    apt-get install -y build-essential && \
    apt-get install -y --no-install-recommends \
        freetds-bin \
        krb5-user \
        ldap-utils \
        libffi6 \
        libsasl2-2 \
        libsasl2-modules \
        libssl1.1 \
        locales  \
        lsb-release \
        sasl2-bin \
        sqlite3 \
        unixodbc && \
    pip install --retries 100 --timeout 300 apache-airflow==1.10.12 \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-1.10.12/constraints-3.7.txt"

RUN mkdir -p /usr/share/man/man1 && \
    apt-get install -y openjdk-11-jre && \
    pip install --retries 100 --timeout 300 pyspark

RUN mkdir -p datalake/crudo/generador/fuente && \
    mkdir -p datalake/procesado/generador/fuente && \
    mkdir -p /airflow/dags && \
    mkdir -p src/spark

COPY src/airflow/dags/. /root/airflow/dags/.
COPY src/spark/. src/spark/.
COPY entrypoint.sh .
COPY datalake/crudo/generador/fuente/20200801/ datalake/crudo/generador/fuente/20200801/

EXPOSE 8080

ENTRYPOINT ["bash", "entrypoint.sh"]

