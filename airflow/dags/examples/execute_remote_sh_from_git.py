# -*- coding: utf-8 -*-
​
from builtins import range
from datetime import timedelta
​
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
​
args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}
​
dag = DAG(
    dag_id='example_bash_operator_3',
    default_args=args,
)
​
consulta=open('/home/usuario/airflow/dags/ssh/sh/ssh_bash.txt','r').read()

# DAG Creation
with DAG('ssh_execute', catchup=False, default_args=default_args) as dag:​
    
    sshPrueba = SSHOperator(
        task_id='sshPrueba',
        command=consulta,
        timeout=20,
        ssh_conn_id="ssh_airflow",
    )
​
run_this