from builtins import range
from datetime import datetime, timedelta, date
import os
import json
import stat
import airflow
from airflow.models import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.models import Variable
from airflow.contrib.hooks import SSHHook

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

with DAG('BigData_Clean_ReceptionEvent_Solr_DAG', schedule_interval="0 8 * * 1-7", catchup=False, default_args=default_args) as dag:
    
    clean_index_solr_acknowledgement = SimpleHttpOperator(
        task_id='post_ack',
        endpoint='http://localhost:9003/api/solr/index/delete?collection=ACKNOWLEDGEMENTS&days=30',
        data=json.dumps({"priority": 5}),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: len(response.json()) == 0,
    )
    
    clean_index_solr_dispatch = SimpleHttpOperator(
        task_id='post_dispatch',
        endpoint='http://localhost:9003/api/solr/index/delete?collection=DISPATCH&days=30',
        data=json.dumps({"priority": 5}),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: len(response.json()) == 0,
    ) 
    
    clean_index_solr_error = SimpleHttpOperator(
        task_id='post_error',
        endpoint='http://localhost:9003/api/solr/index/delete?collection=ERROR&days=30',
        data=json.dumps({"priority": 5}),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: len(response.json()) == 0,
    )
   
    clean_index_solr_heart_beat = SimpleHttpOperator(
        task_id='post_heart',
        endpoint='http://localhost:9003/api/solr/index/delete?collection=HEART_BEAT&days=7',
        data=json.dumps({"priority": 5}),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: len(response.json()) == 0,
    )

    clean_index_solr_initialization = SimpleHttpOperator(
        task_id='post_init',
        endpoint='http://localhost:9003/api/solr/index/delete?collection=INITIALIZATION&days=30',
        data=json.dumps({"priority": 5}),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: len(response.json()) == 0,
    )

    clean_index_solr_reception = SimpleHttpOperator(
        task_id='post_recep',
        endpoint='http://localhost:9003/api/solr/index/delete?collection=RECEPTION&days=30',
        data=json.dumps({"priority": 5}),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: len(response.json()) == 0,
    )

    clean_index_solr_state_change = SimpleHttpOperator(
        task_id='post_state',
        endpoint='http://localhost:9003/api/solr/index/delete?collection=STATE_CHANGE&days=30',
        data=json.dumps({"priority": 5}),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: len(response.json()) == 0,
    )

    clean_index_solr_warning = SimpleHttpOperator(
        task_id='post_warn',
        endpoint='http://localhost:9003/api/solr/index/delete?collection=WARNING&days=30',
        data=json.dumps({"priority": 5}),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: len(response.json()) == 0,
    )

    clean_index_solr_error   