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
    'retry_delay': timedelta(minutes=5)
}

with DAG('MKT_Talon_DAG', schedule_interval='0 6 * * *', catchup=False, default_args=default_args) as dag:
    # Extraccion de datos desde servicio talon
    get_data_from_talon_service = SSHOperator(
        task_id="get_data_from_talon_service",
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

    copy_data_from_lfs_to_hdfs_campaigns = SSHOperator(
        task_id = "copy_data_from_lfs_to_hdfs_campaigns",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/carga_talon_centos_to_hdfs.sh "campaigns" "/SQS/Talon/Campaigns/" "json"
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_datanode1_ti"
    )

    copy_data_from_lfs_to_hdfs_coupons = SSHOperator(
        task_id = "copy_data_from_lfs_to_hdfs_coupons",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/carga_talon_centos_to_hdfs.sh "coupons" "/SQS/Talon/CouponsCsv/" "csv"
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_datanode1_ti"
    )

    checkpoint = DummyOperator(
        task_id='checkpoint')

    process_data_and_move_to_s3_campaigns = SSHOperator(
        task_id = "process_data_and_move_to_s3_campaigns",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_datanode1_ti"
    )

    process_data_and_move_to_s3_coupons = SSHOperator(
        task_id = "process_data_and_move_to_s3_coupons",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_datanode1_ti"
    )

    # Ver con Carlos en parseo de campos
    # GZIP
    # SH Para los talend, los pasa carlos a Nico --> Willy le explica 
    # shh credentials, pasamos la clave publica, Diego se la pasa a Carlos
    # usuario de amazon para Santiago y Nicolas
    # Columnas del ODS, de que archivos vienen... 

    get_data_from_talon_service >> validationGetDataTalonService >> \
        [copy_data_from_lfs_to_hdfs_campaigns,copy_data_from_lfs_to_hdfs_coupons] >> checkpoint >> \
            [process_data_and_move_to_s3_campaigns,process_data_and_move_to_s3_coupons]
