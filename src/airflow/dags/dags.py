from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator


default_args = {
    'owner': 'TDT',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 1),
    'retries': 0,
}

with DAG('ERP_Preprocessing',
         description='_',
         default_args=default_args,
         schedule_interval=timedelta(days=7),
         max_active_runs=1) as erp_dag:

    path_tmpl = 'datalake/crudo/generador/fuente/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y%m%d") }}/'
    file_sensor = FileSensor(
        task_id='file_sensor',
        poke_interval=30,
        filepath=os.path.abspath(path_tmpl)
    )

    submit_erp_app = SparkSubmitOperator(
        task_id='submit_spark_erp_app',
        conn_id='spark_local',
        application='src/spark/erp_preprocessing.py',
        env_vars={
            'AIRFLOW_DS': '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y%m%d") }}'
        }
    )

    submit_ti_app = SparkSubmitOperator(
        task_id='submit_spark_ti_app',
        conn_id='spark_local',
        application='src/spark/ti_preprocessing.py',
        env_vars={
            'AIRFLOW_DS': '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y%m%d") }}'
        }
    )

    file_sensor >> [submit_erp_app, submit_ti_app]
