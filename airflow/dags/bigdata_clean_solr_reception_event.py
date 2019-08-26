from builtins import range
from datetime import datetime, timedelta, date
import os
import json
import stat
import airflow
import requests
from airflow.models import DAG
from airflow.sensors.http_sensor import HttpSensor
from airflow.models import Variable
from airflow.contrib.hooks import SSHHook
from airflow.operators.python_operator import PythonOperator

# Variables

# Params DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 9),
    'email': ['diego.pietruszka@pedidosya.com','carlos.cristoforone@pedidosya.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=5)
}

def delete_solr_files(**kwargs):
    print("Collection: " + kwargs['collection'])
    # definimos endpoint
    API_ENDPOINT = "http://localhost:9003/api/solr/index/delete?collection={0}&days=28".format(kwargs['collection'])
    # enviamos post request
    r = requests.post(url = API_ENDPOINT)

with DAG('BigData_Clean_ReceptionEvent_Solr_DAG', schedule_interval="0 8 * * 1-7", catchup=False, default_args=default_args) as dag:

    clean_index_solr_error = PythonOperator(
        task_id='clean_index_solr_error',
        provide_context=True,
        python_callable=delete_solr_files,
        op_kwargs={'collection': 'ERROR'},
        dag=dag
    )
   
    clean_index_solr_error   