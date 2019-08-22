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
    'start_date': datetime(2019, 8, 1),
    'email': ['carlos.cristoforone@pedidosya.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

# Funciones
def validate_message():
    #  ---- Logica validacion ----
    # Generacion archivo
    # Delta > x 
    print('OK!')

with DAG('BigData_Hue_TablesAudit', schedule_interval='0 12-16/2 * * *', catchup=False, default_args=default_args) as dag:
    # Llamado al auditor de tablas de hue
    runHueTableAudit = BashOperator(
    task_id='HueTableAudit',
    bash_command="""
        /home/hduser/backendbi-audit/HueTables/hueaudit.sh
        """
    )

    runHueTableAudit

