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

fecha_actual = date.today().strftime("%Y%m%d")
path_campaigns_talon = Variable.get('path_campaigns_talon') + fecha_actual
path_coupons_talon = Variable.get('path_coupons_talon') + fecha_actual


# Params DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 9),
    'email': ['diego.pietruszka@pedidosya.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=60)
}

with DAG('Talon_DAG_Testing', schedule_interval='0 */2 * * *', catchup=False, default_args=default_args) as dag:
    # Extraccion de datos desde servicio talon
    getDataTalonService = SSHOperator(
        task_id="getDataTalonService",
        command="""
        /usr/bin/bash /home/hduser/backendbi-procesos/start_airflow_talon.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_datanode1_ti"
    )

    validationGetDataTalonService = SSHOperator(
        task_id = "validationGetDataTalonService",
        command="""
        /usr/bin/bash /home/hduser/airflow-scripts/audit.sh audit_talon_service.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_datanode1_ti"
    )

    getDataTalonService >> validationGetDataTalonService

