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


#sshHook = SSHHook(conn_id="ssh_hadoop_datanode1")

# Params DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 9),
    'email': ['diego.pietruszka@pedidosya.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 15,
    'retry_delay': timedelta(minutes=60)
}

# Funciones
def validate_message():
    #  ---- Logica validacion ----
    # Generacion archivo
    # Delta > x 
    print('OK!')

with DAG('Talon_DAG', schedule_interval='0 0 */2 * * *', catchup=False, default_args=default_args) as dag:
    # Extraccion de datos desde servicio talon
    getDataTalonService = SSHOperator(
        task_id="getDataTalonService",
        command="""
        /usr/bin/bash /home/hduser/backendbi-procesos/start_backendbi-procesos_weekly.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_datanode1_ti"
    )

    # Mensaje OK
    validationGetDataTalonService = PythonOperator(
        task_id = "validationGetDataTalonService",
        python_callable = validate_message
    )

    getDataTalonService >> validationGetDataTalonService

