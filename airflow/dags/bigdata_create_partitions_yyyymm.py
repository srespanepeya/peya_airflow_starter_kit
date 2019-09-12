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

#Dummy operator
from airflow.operators.dummy_operator import DummyOperator

# Params DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'wait_for_downstream': False,
    'start_date': datetime(2019, 7, 9),
    'email': ['diego.pietruszka@pedidosya.com','carlos.cristoforone@pedidosya.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('BigData_Create_Partitions_yyyyMM_DAG', schedule_interval="0 0 1 * *", catchup=False, default_args=default_args) as dag:
    
    rmbi = SSHOperator(
        task_id="create_aniomes_rmbi",
        command="""
        /usr/bin/bash /home/hduser/hive/scripts/create_partition_aniomes.sh
        """,
        timeout = 60,
        ssh_conn_id = "ssh_hadoop_resmanager_bi",
        dag = dag
    )

    rmba = SSHOperator(
        task_id="create_aniomes_rmba",
        command="""
        /usr/bin/bash /home/hduser/hive/scripts/create_partition_aniomes.sh
        """,
        timeout = 60,
        ssh_conn_id = "ssh_hadoop_resmanager_ba",
        dag = dag
    )
   
    prod = SSHOperator(
        task_id="create_aniomes_rmprod",
        command="""
        /usr/bin/bash /home/hduser/hive/scripts/create_partition_aniomes.sh
        """,
        timeout = 60,
        ssh_conn_id = "ssh_hadoop_resmanager_prod",
        dag = dag
    )

    dl = SSHOperator(
        task_id="create_aniomes_rmmark",
        command="""
        /usr/bin/bash /home/hduser/hive/scripts/create_partition_aniomes.sh
        """,
        timeout = 60,
        ssh_conn_id = "ssh_hadoop_resmanager_mark",
        dag = dag
    )
    
    rmti = SSHOperator(
        task_id="create_aniomes_rmti",
        command="""
        /usr/bin/bash /home/hduser/hive/scripts/create_partition_aniomes.sh
        """,
        timeout = 60,
        ssh_conn_id = "ssh_hadoop_resmanager_ti",
        dag = dag
    )