#!/bin/bash

airflow initdb
airflow connections --add --conn_id spark_local --conn_type spark --conn_host local
airflow scheduler >& airflow_scheduler.log &
airflow webserver -p 8080 >& airflow_webserver.log &

tail -f airflow_scheduler.log
