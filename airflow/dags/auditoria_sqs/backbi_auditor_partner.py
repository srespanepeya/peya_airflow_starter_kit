from builtins import range
from datetime import datetime, timedelta
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


#sshHook = SSHHook(conn_id="ssh_hadoop_datanode2_ti")

# Params DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 15),
    'email': ['carlos.cristoforone@pedidosya.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}

# Funciones
def validate_message():
    #  ---- Logica validacion ----
    # Generacion archivo
    # Delta > x 
    print('OK!')

with DAG('BackendBI_Auditoria_Partners', schedule_interval='0 6 * * *', catchup=False, default_args=default_args) as dag:
    # Llamado al auditor de partners en datanode 2
    runPartnersAuditor = SSHOperator(
        task_id="PartnersAuditor",
        command="""
        /usr/bin/bash /home/hduser/backendbi-audit/Partner/PartnersSyncValidation/PartnersSyncValidation_run.sh
        """,
        timeout = 60,
        ssh_conn_id = "ssh_hadoop_datanode2_ti"
    )

    runPartnersAuditor

