from builtins import range
from datetime import datetime, timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks import SSHHook


sshHook = SSHHook(conn_id="ssh_hadoop_datanode1")

# Params DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 9),
    'email': ['bigdata@pedidosya.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 15,
    'retry_delay': timedelta(minutes=60)
}

# Funciones
def validate():
    #  ---- Logica validacion ----
    # Generacion archivo
    # Delta > x 
    print('OK!')

with DAG('Talon_DAG', schedule_interval='00 12 * * *', catchup=False, default_args=default_args) as dag:
    # Extraccion de datos desde servicio talon
    getDataTalonService = SSHOperator(
        task_id="getDataTalonService",
        bash_command="""
        /home/hduser/backendbi-procesos/start_backendbi-procesos_weekly.sh
        """,
        ssh_hook=sshHook
    )

    # Mensaje OK
    validate = PythonOperator(
        task_id = messageOK,
        python_callable = validate
    )

    getDataTalonService >> validate

