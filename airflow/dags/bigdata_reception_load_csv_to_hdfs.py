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
    'retry_delay': timedelta(minutes=5)
}

with DAG('BigData_Reception_Solr_To_HDFS', schedule_interval=None, catchup=False, default_args=default_args) as dag:

    begin_task = DummyOperator(
        task_id='begin_task',
        dag=dag)

    # Extraccion de datos desde servicio solr
    extract_acknowledgement = SSHOperator(
        task_id="get_acknowledgement_from_solr_service",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/airflow-scripts/reception/extract_evento_to_csv.sh acknowledgement
        /usr/bin/bash /home/hduser/airflow-scripts/audit.sh audit_talon_service.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_namenode_ti"
    )

    # Extraccion de datos desde servicio solr
    extract_warning = SSHOperator(
        task_id="get_warning_from_solr_service",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/airflow-scripts/reception/extract_evento_to_csv.sh warning
        /usr/bin/bash /home/hduser/airflow-scripts/audit.sh audit_talon_service.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_namenode_ti"
    )

    # Extraccion de datos desde servicio solr
    extract_heart_beat = SSHOperator(
        task_id="get_heart_beat_from_solr_service",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/airflow-scripts/reception/extract_evento_to_csv.sh heart_beat
        /usr/bin/bash /home/hduser/airflow-scripts/audit.sh audit_talon_service.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_namenode_ti"
    )

    # Extraccion de datos desde servicio solr
    extract_initialization = SSHOperator(
        task_id="get_initialization_from_solr_service",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/airflow-scripts/reception/extract_evento_to_csv.sh initialization
        /usr/bin/bash /home/hduser/airflow-scripts/audit.sh audit_talon_service.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_namenode_ti"
    )

    # Extraccion de datos desde servicio solr
    extract_reception = SSHOperator(
        task_id="get_reception_from_solr_service",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/airflow-scripts/reception/extract_evento_to_csv.sh reception
        /usr/bin/bash /home/hduser/airflow-scripts/audit.sh audit_talon_service.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_namenode_ti"
    )

    # Extraccion de datos desde servicio solr
    extract_state_change = SSHOperator(
        task_id="get_state_change_from_solr_service",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/airflow-scripts/reception/extract_evento_to_csv.sh state_change
        /usr/bin/bash /home/hduser/airflow-scripts/audit.sh audit_talon_service.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_namenode_ti"
    )

    # Extraccion de datos desde servicio solr
    extract_dispatch = SSHOperator(
        task_id="get_dispatch_from_solr_service",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/airflow-scripts/reception/extract_evento_to_csv.sh dispatch
        /usr/bin/bash /home/hduser/airflow-scripts/audit.sh audit_talon_service.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_namenode_ti"
    )

    # Extraccion de datos desde servicio solr
    extract_error = SSHOperator(
        task_id="get_error_from_solr_service",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/airflow-scripts/reception/extract_evento_to_csv.sh error
        /usr/bin/bash /home/hduser/airflow-scripts/audit.sh audit_talon_service.sh
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
        /usr/bin/bash /home/hduser/spark/apps/airflow-scripts/reception/extract_evento_to_csv.sh acknowledgement
        /usr/bin/bash /home/hduser/airflow-scripts/audit.sh audit_talon_service.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_namenode_ti"
    )

    # Extraccion de datos desde servicio solr
    write_warning_hdfs = SSHOperator(
        task_id="write_warning_hdfs",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/airflow-scripts/reception/extract_evento_to_csv.sh warning
        /usr/bin/bash /home/hduser/airflow-scripts/audit.sh audit_talon_service.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_namenode_ti"
    )

    # Extraccion de datos desde servicio solr
    write_heart_beat_hdfs = SSHOperator(
        task_id="write_heart_beat_hdfs",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/airflow-scripts/reception/extract_evento_to_csv.sh heart_beat
        /usr/bin/bash /home/hduser/airflow-scripts/audit.sh audit_talon_service.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_namenode_ti"
    )

    # Extraccion de datos desde servicio solr
    write_initialization_hdfs = SSHOperator(
        task_id="write_initialization_hdfs",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/airflow-scripts/reception/extract_evento_to_csv.sh initialization
        /usr/bin/bash /home/hduser/airflow-scripts/audit.sh audit_talon_service.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_namenode_ti"
    )

    # Extraccion de datos desde servicio solr
    write_reception_hdfs = SSHOperator(
        task_id="write_reception_hdfs",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/airflow-scripts/reception/extract_evento_to_csv.sh reception
        /usr/bin/bash /home/hduser/airflow-scripts/audit.sh audit_talon_service.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_namenode_ti"
    )

    # Extraccion de datos desde servicio solr
    write_state_change_hdfs = SSHOperator(
        task_id="write_state_change_hdfs",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/airflow-scripts/reception/extract_evento_to_csv.sh state_change
        /usr/bin/bash /home/hduser/airflow-scripts/audit.sh audit_talon_service.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_namenode_ti"
    )

    # Extraccion de datos desde servicio solr
    write_dispatch_hdfs = SSHOperator(
        task_id="write_dispatch_hdfs",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/airflow-scripts/reception/extract_evento_to_csv.sh dispatch
        /usr/bin/bash /home/hduser/airflow-scripts/audit.sh audit_talon_service.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_namenode_ti"
    )

    # Extraccion de datos desde servicio solr
    write_error_hdfs = SSHOperator(
        task_id="write_error_hdfs",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/airflow-scripts/reception/extract_evento_to_csv.sh error
        /usr/bin/bash /home/hduser/airflow-scripts/audit.sh audit_talon_service.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_namenode_ti"
    )

    check_point_2 = DummyOperator(
        task_id='check_point_2',
        dag=dag)    

    begin_task >> [extract_acknowledgement,extract_warning,extract_heart_beat,extract_initialization,extract_reception,extract_state_change,extract_dispatch,extract_error] >> check_point_1 >> [write_acknowledgement_hdfs,write_warning_hdfs,write_heart_beat_hdfs,write_initialization_hdfs,write_reception_hdfs,write_state_change_hdfs,write_dispatch_hdfs,write_error_hdfs] >> check_point_2
