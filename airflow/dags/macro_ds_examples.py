from builtins import range
from datetime import datetime, timedelta, date
import os
import stat
import airflow
from airflow import macros
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

# Params DAG
default_args = {
    'owner': 'bigdata',
    'depends_on_past': False,
    'wait_for_downstream': False,
    'start_date': datetime(2019, 9, 1),
    'email': ['diego.pietruszka@pedidosya.com','carlos.cristoforone@pedidosya.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=5)
}

#partition_date_0 = datetime.now()
aDayAgo = {{ ds }}
print('aDayAgo = {{ ds }} -> aDayAgo is {0}'.format(aDayAgo))
aDayAgoWithoutDash = {{ ds_nodash }}
print('aDayAgoWithoutDash = {{ ds_nodash }} -> aDayAgoWithoutDash is {0}'.format(aDayAgo))

twoDaysAgo = ds_add(aDayAgo, -1)
print('twoDaysAgo = ds_add(aDayAgo, -1) -> twoDaysAgo is {0}'.format(twoDaysAgo))
twoDaysAgoWithoutDash= ds_format(twoDaysAgo, "%Y-%m-%d", "%Y%m%d")
print('twoDaysAgoWithoutDash = ds_add(twoDaysAgo, "%Y-%m-%d", "%Y%m%d") -> twoDaysAgoWithoutDash is {0}'.format(twoDaysAgoWithoutDash))

threeDaysAgo = ds_add(twoDaysAgo, -1)
print('threeDaysAgo = ds_add(twoDaysAgo, -1) -> threeDaysAgo is {0}'.format(threeDaysAgo))
threeDaysAgoWithoutDash= ds_format(threeDaysAgo, "%Y-%m-%d", "%Y%m%d")
print('threeDaysAgoWithoutDash = ds_add(threeDaysAgo, "%Y-%m-%d", "%Y%m%d") -> threeDaysAgoWithoutDash is {0}'.format(threeDaysAgoWithoutDash))

fourDaysAgo = ds_add(threeDaysAgo, -1)
print('fourDaysAgo = ds_add(threeDaysAgo, -1) -> fourDaysAgo is {0}'.format(threeDaysAgo))
fourDaysAgoWithoutDash= ds_format(fourDaysAgo, "%Y-%m-%d", "%Y%m%d")
print('fourDaysAgoWithoutDash = ds_add(fourDaysAgo, "%Y-%m-%d", "%Y%m%d") -> fourDaysAgoWithoutDash is {0}'.format(fourDaysAgoWithoutDash))

dummy = DummyOperator(task_id='dummy', dag=dag)

with DAG('Example_Macro_ds', schedule_interval="0 7 * * 1-7", catchup=True, default_args=default_args) as dag:
