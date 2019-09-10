from builtins import range
from datetime import datetime, timedelta, date
import os
import stat
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.contrib.hooks import SSHHook

# Params DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'wait_for_downstream': False,
    'start_date': datetime(2019, 7, 9),
    'email': ['diego.pietruszka@pedidosya.com','carlos.cristoforone@pedidosya.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('BigData_Index_Orders_LogEvents_Solr_DAG', schedule_interval="*/3 * * * 1-7", catchup=False, default_args=default_args) as dag:
    
    event = BashOperator(
        task_id='write_index_solr_ord_logistic_events',
        bash_command="""
            /home/hduser/backendbi-procesos/BigDataOrdersToSolr/LogisticEvent.sh
            """,
        dag = dag
    )

