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

# Params DAG
default_args = {
    'owner': 'bigdata',
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 14),
    'email': ['diego.pietruszka@pedidosya.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 20,
    'retry_delay': timedelta(minutes=10)
}

# Variables
try:
    dir_csv_reception_events = Variable.get('dir_csv_reception_events')
except:
    # En caso de fallos, seteamos valores por defecto
    dir_csv_reception_events = "/home/hduser/hdfs/data/solr/"

with DAG('BigData_Reception_Solr_To_HDFS', schedule_interval=None, catchup=False, default_args=default_args) as dag:

    begin_task = DummyOperator(
        task_id='begin_task',
        dag=dag)

    # Extraccion de datos desde servicio solr
    extract_acknowledgement = SSHOperator(
        task_id="get_acknowledgement_from_solr_service",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/airflow-scripts/reception/extract_evento_to_csv.sh acknowledgements timestamp
        /usr/bin/bash /home/hduser/spark/apps/airflow-scripts/reception/audit_extract_data_from_solr.sh acknowledgements
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_namenode_ti"
    )

    check_point_1 = DummyOperator(
        task_id='check_point_1',
        dag=dag)

    # Extraccion de datos desde servicio solr
    write_acknowledgement_hdfs = SSHOperator(
        task_id="write_acknowledgement_hdfs",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/airflow-scripts/reception/load_event_from_csv_to_hdfs.sh acknowledgement
        /usr/bin/bash /home/hduser/airflow-scripts/audit.sh audit_talon_service.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_namenode_ti"
    )

    check_point_2 = DummyOperator(
        task_id='check_point_2',
        dag=dag)    

    begin_task >> [extract_acknowledgement] >> check_point_1 >> [write_acknowledgement_hdfs] >> check_point_2
