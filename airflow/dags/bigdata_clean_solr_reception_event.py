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
try:
    API_HOST = Variable.get('bi_api_service_host')
    API_PORT = Variable.get('bi_api_service_port')
except:
    # En caso de fallos, seteamos valores por defecto
    API_HOST = "10.0.91.124"
    API_PORT = "9003"

PROTOCOLO = "http://"
API_ENDPOINT = "{0}:{1}".format(API_HOST, API_PORT)    


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
    print("Days: " + kwargs['days'])
    print("Date Field" + kwargs['dateField'])
    # definimos endpoint
    API_REQUEST = "{0}{1}/api/solr/index/delete?collection={2}&days={3}&dateField={4}".format(PROTOCOLO, API_ENDPOINT, kwargs['collection'], kwargs['days'], kwargs['dateField'])
    print(API_REQUEST)
    # enviamos post request
    r = requests.post(url = API_REQUEST)

with DAG('BigData_Clean_ReceptionEvent_Solr_DAG', schedule_interval="0 8 * * 1-7", catchup=False, default_args=default_args) as dag:

    clean_index_solr_ack = PythonOperator(
        task_id='clean_index_solr_ack',
        provide_context=True,
        python_callable=delete_solr_files,
        op_kwargs={'collection': 'ACKNOWLEDGEMENTS', 'dateField': 'timestamp', 'days': '30'},
        dag=dag
    )

    clean_index_solr_dispatch = PythonOperator(
        task_id='clean_index_solr_dispatch',
        provide_context=True,
        python_callable=delete_solr_files,
        op_kwargs={'collection': 'DISPATCH', 'dateField': 'dispatchDate', 'days': '30'},
        dag=dag
    )

    clean_index_solr_error = PythonOperator(
        task_id='clean_index_solr_error',
        provide_context=True,
        python_callable=delete_solr_files,
        op_kwargs={'collection': 'ERROR', 'dateField': 'timestamp', 'days': '30'},
        dag=dag
    )

    clean_index_solr_heartbeat = PythonOperator(
        task_id='clean_index_solr_heartbeat',
        provide_context=True,
        python_callable=delete_solr_files,
        op_kwargs={'collection': 'HEART_BEAT', 'dateField': 'timestamp', 'days': '7'},
        dag=dag
    )

    clean_index_solr_init = PythonOperator(
        task_id='clean_index_solr_init',
        provide_context=True,
        python_callable=delete_solr_files,
        op_kwargs={'collection': 'INITIALIZATION', 'dateField': 'timestamp', 'days': '30'},
        dag=dag
    )

    clean_index_solr_recep = PythonOperator(
        task_id='clean_index_solr_recep',
        provide_context=True,
        python_callable=delete_solr_files,
        op_kwargs={'collection': 'RECEPTION', 'dateField': 'timestamp', 'days': '30'},
        dag=dag
    )
   
    clean_index_solr_state_change = PythonOperator(
        task_id='clean_index_solr_state_change',
        provide_context=True,
        python_callable=delete_solr_files,
        op_kwargs={'collection': 'STATE_CHANGE', 'dateField': 'timestamp', 'days': '30'},
        dag=dag
    )

    clean_index_solr_warn = PythonOperator(
        task_id='clean_index_solr_warn',
        provide_context=True,
        python_callable=delete_solr_files,
        op_kwargs={'collection': 'WARNING', 'dateField': 'timestamp', 'days': '30'},
        dag=dag
    )

    clean_index_solr_ack >> clean_index_solr_dispatch >> clean_index_solr_error >> clean_index_solr_heartbeat >> clean_index_solr_init >> clean_index_solr_recep >> clean_index_solr_state_change >> clean_index_solr_warn   