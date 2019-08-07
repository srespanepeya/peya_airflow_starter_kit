from builtins import range
from datetime import datetime, timedelta, date
import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks import SSHHook


# Variables
path_reception_event = string(Variable.get('path_reception_event'))
usr_solr = string(Variable.get('usr_solr'))
pass_solr = string(Variable.get('pass_solr'))
index = "INITIALIZATION"


# Params DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 9),
    'email': ['diego.pietruszka@pedidosya.com','carlos.cristoforone@pedidosya.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

with DAG('BigData_Writer_Initialization_Solr_DAG', schedule_interval=None, catchup=False, default_args=default_args) as dag:
    # Extraccion de datos desde servicio talon
    write_index_solr_initialization = SSHOperator(
        task_id="write_index_solr_initialization",
        command="""
        /usr/bin/bash /home/hduser/solr/apps/airflow/INDEX_WRITER_MINUTO.sh writer-A0 {0} {1} {2} {3}   
        """.format(usr_solr,pass_solr,index,path_reception_event),
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_datanode1_ti"
    )

get_data_from_talon_service    



