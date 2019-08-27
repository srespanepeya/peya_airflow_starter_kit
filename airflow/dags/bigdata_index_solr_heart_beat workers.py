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
#Dummy operator
from airflow.operators.dummy_operator import DummyOperator

# Variables

# Params DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 9),
    'email': ['diego.pietruszka@pedidosya.com','carlos.cristoforone@pedidosya.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=5)
}

with DAG('BigData_Index_Heart_Beat_Worker_Solr_DAG', schedule_interval="*/3 * * * 1-7", catchup=False, default_args=default_args) as dag:

    dummy = DummyOperator(
        task_id='dummy_op',
        dag=dag
    )
    
    wr0 = SSHOperator(
        task_id="write_index_solr_heart_beat_wr0",
        command="""
        /usr/bin/bash /home/hduser/backendbi-procesos/BigDataReceptionEventToSolr/HEART_BEAT_MINUTOS_0.sh
        """,
        timeout = 1600,
        ssh_conn_id = "ssh_hadoop_datanode1_ti",
        dag=dag
    )
    wr1 = SSHOperator(
        task_id="write_index_solr_heart_beat_wr1",
        command="""
        /usr/bin/bash /home/hduser/backendbi-procesos/BigDataReceptionEventToSolr/HEART_BEAT_MINUTOS_1.sh
        """,
        timeout = 1600,
        ssh_conn_id = "ssh_hadoop_datanode1_ti",
        dag=dag
    )
    wr2 = SSHOperator(
        task_id="write_index_solr_heart_beat_wr2",
        command="""
        /usr/bin/bash /home/hduser/backendbi-procesos/BigDataReceptionEventToSolr/HEART_BEAT_MINUTOS_2.sh
        """,
        timeout = 1600,
        ssh_conn_id = "ssh_hadoop_datanode1_ti",
        dag=dag
    )
    wr3 = SSHOperator(
        task_id="write_index_solr_heart_beat_wr3",
        command="""
        /usr/bin/bash /home/hduser/backendbi-procesos/BigDataReceptionEventToSolr/HEART_BEAT_MINUTOS_3.sh
        """,
        timeout = 1600,
        ssh_conn_id = "ssh_hadoop_datanode1_ti",
        dag=dag
    )
    wr4 = SSHOperator(
        task_id="write_index_solr_heart_beat_wr4",
        command="""
        /usr/bin/bash /home/hduser/backendbi-procesos/BigDataReceptionEventToSolr/HEART_BEAT_MINUTOS_4.sh
        """,
        timeout = 1600,
        ssh_conn_id = "ssh_hadoop_datanode1_ti",
        dag=dag
    )

dummy >> [wr0,wr1,wr2,wr3,wr4]   