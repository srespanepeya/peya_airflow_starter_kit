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

#casteo la variable obtenida de las Variables generales de airflow para obtener la ruta del repo    
try:
    git_path=string(Variable.get('git_bi_bigdata_path'));
except expression as identifier:
    git_path='/root/airflow_extra/bigdata-airflow'

#cargo en una variable el sh a ejecutar con el bash con la ruta especifica donde esta en el server
script=open('{0}/airflow/dags/indexadores_solR/order_logistic_events/sh/LogisticEvent.sh'.format(git_path),'r').read()

with DAG('BigData_Index_Orders_LogEvents_Solr_DAG', schedule_interval="*/3 * * * 1-7", catchup=False, default_args=default_args) as dag:
    
    event = BashOperator(
        task_id='write_index_solr_ord_logistic_events',
        bash_command=script,
        dag = dag
    )

    #"""
    #/home/hduser/backendbi-procesos/BigDataOrdersToSolr/LogisticEvent.sh
    #"""