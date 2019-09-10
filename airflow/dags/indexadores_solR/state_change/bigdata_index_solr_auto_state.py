from builtins import range
from datetime import datetime, timedelta, date
import os
import stat
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks import SSHHook

# Variables

#Dummy operator
from airflow.operators.dummy_operator import DummyOperator

# Params DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'wait_for_downstream': False,
    'start_date': datetime(2019, 7, 9),
    'email': ['diego.pietruszka@pedidosya.com','carlos.cristoforone@pedidosya.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=5)
}

with DAG('BigData_Index_ReceptionEvent_State_Solr_DAG', schedule_interval="*/3 * * * 1-7", catchup=False, default_args=default_args) as dag:
   
    state = SSHOperator(
        task_id="write_index_solr_state_change",
        command="""
        /usr/bin/bash /home/hduser/backendbi-procesos/BigDataReceptionEventToSolr/STATE_CHANGE.sh
        """,
        timeout = 60,
        ssh_conn_id = "ssh_hadoop_datanode1_ti",
        dag = dag
    )

