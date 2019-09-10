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
​
# [START howto_operator_bash]
run_this = BashOperator(
    task_id='run_after_loop',
    bash_command=consulta,
    dag=dag,
)
# [END howto_operator_bash]
​
run_this
