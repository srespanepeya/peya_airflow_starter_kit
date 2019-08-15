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

try:
    path_reception_event = string(Variable.get('path_reception_event'))
except:
    path_reception_event = "/home/hduser/hdfs/data/solr/SQS/ReceptionEvent" 

try:
    usr_solr = string(Variable.get('usr_solr'))
except:
    usr_solr = "solr"

try:
    pass_solr = string(Variable.get('pass_solr'))
except:
    pass_solr = "H5XZthKmPVzGd"

index = "RECEPTION"

# Params DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 9),
    'email': ['diego.pietruszka@pedidosya.com','carlos.cristoforone@pedidosya.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=5)
}

with DAG('BigData_Writer_Reception_Solr_DAG', schedule_interval="* * * * 1-7", catchup=False, default_args=default_args) as dag:
    # Extraccion de datos desde servicio talon
    write_index_solr_reception = SSHOperator(
        task_id="write_index_solr_reception",
        command="""
        /usr/bin/bash /home/hduser/solr/apps/airflow/INDEX_WRITER_MINUTO.sh writer-A0 {0} {1} {2} {3}
        /usr/bin/bash /home/hduser/solr/apps/airflow/INDEX_WRITER_MINUTO.sh writer-A1 {0} {1} {2} {3}
        /usr/bin/bash /home/hduser/solr/apps/airflow/INDEX_WRITER_MINUTO.sh writer-A2 {0} {1} {2} {3}
        /usr/bin/bash /home/hduser/solr/apps/airflow/INDEX_WRITER_MINUTO.sh writer-A3 {0} {1} {2} {3}
        /usr/bin/bash /home/hduser/solr/apps/airflow/INDEX_WRITER_MINUTO.sh writer-A4 {0} {1} {2} {3}   
        """.format(usr_solr,pass_solr,index,path_reception_event),
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_datanode1_ti"
    )

write_index_solr_reception   