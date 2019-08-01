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


try:
    git_repo_path = string(Variable.get('git_mkt_path'))
except:
    git_repo_path = "/root/airflow_extra/peya_airflow_starter_kit"

dag_path="{0}/airflow/dags/MKT_Talon_DAG".format(git_repo_path)
py_path= "{0}/py".format(dag_path)


today_nodash = date.today().strftime("%Y%m%d")
path_campaigns_talon = Variable.get('path_campaigns_talon') + today_nodash
path_coupons_talon = Variable.get('path_coupons_talon') + today_nodash

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

with DAG('MKT_Talon_DAG', schedule_interval=None, catchup=False, default_args=default_args) as dag:
    # Extraccion de datos desde servicio talon
    get_data_from_talon_service = SSHOperator(
        task_id="get_data_from_talon_service",
        command="""
        /usr/bin/bash /home/hduser/backendbi-procesos/start_airflow_talon_batch.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_datanode1_ti"
    )

    validation_get_data_talon_service = SSHOperator(
        task_id = "validation_get_data_talon_service",
        command="""
        /usr/bin/bash /home/hduser/airflow-scripts/audit.sh audit_talon_service.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_datanode1_ti"
    )

    copy_data_from_lfs_to_hdfs_campaigns = SSHOperator(
        task_id = "copy_data_from_lfs_to_hdfs_campaigns",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/carga_talon_centos_to_hdfs_batch.sh "campaigns" "/home/hduser/hdfs/data/solr/SQS/Talon/" "json"
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_datanode1_ti"
    )

    copy_data_from_lfs_to_hdfs_coupons = SSHOperator(
        task_id = "copy_data_from_lfs_to_hdfs_coupons",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/carga_talon_centos_to_hdfs_batch.sh "coupons" "/home/hduser/hdfs/data/solr/SQS/Talon/" "csv"
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_datanode1_ti"
    )

    validation_copy_data_from_lfs_to_hdfs = SSHOperator(
        task_id="validation_copy_data_from_lfs_to_hdfs",
        command="""
        /usr/bin/bash /home/hduser/airflow-scripts/audit_talon_fs_to_hdfs.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_datanode1_ti"
    )

    process_data_and_move_to_s3_campaigns = BashOperator(
        task_id='process_data_and_move_to_s3_campaigns',
        bash_command="""
        echo "--->Begin BATCH MKT Campaigns"
        chmod 755 {0}/mkt_process_campaigns_to_s3.py
        /home/hduser/spark/bin/spark-submit --master spark://hadoop-namenode-ti:7077 --driver-memory 4G --driver-cores 4 --executor-memory 4G --conf spark.cores.max=4 {0}/mkt_process_campaigns_to_s3.py
        echo "<---End BATCH MKT Campaigns"
        """.format(py_path)
    )

    process_data_and_move_to_s3_coupons = BashOperator(
        task_id='process_data_and_move_to_s3_coupons',
        bash_command="""
        echo "--->Begin BATCH MKT Coupons"
        chmod 755 {0}/mkt_process_coupons_to_s3.py
        /home/hduser/spark/bin/spark-submit --master spark://hadoop-namenode-ti:7077 --driver-memory 10G --driver-cores 8 --executor-memory 10G --conf spark.cores.max=8 {0}/mkt_process_coupons_to_s3.py
        echo "<---End BATCH MKT Coupons"
        """.format(py_path)
    )

    dwh_load_coupons_from_s3 = SSHOperator(
        task_id="dwh_get_coupons_from_s3",
        command="""
        /usr/bin/bash /home/peya/TALEND/TESTING/Vouchers/Data/Prueba_Data_Talon_Coupons/Prueba_Data_Talon_Coupons_run.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_talend_process_server"
    )

    dwh_generate_fact_talon_coupons = SSHOperator(
        task_id="dwh_process_fact_talon_coupons",
        command="""
        /usr/bin/bash /home/peya/TALEND/TESTING/Vouchers/Fact/Prueba_Fact_Talon_Coupons/Prueba_Fact_Talon_Coupons_run.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_talend_process_server"
    )


    # sftp://peya@localhost:8001/home/peya/TALEND/TESTING/Vouchers/Fact/Prueba_Fact_Talon_Coupons/Prueba_Fact_Talon_Coupons_run.sh
    # Ver con Carlos en parseo de campos
    # GZIP
    # SH Para los talend, los pasa carlos a Nico --> Willy le explica 
    # shh credentials, pasamos la clave publica, Diego se la pasa a Carlos
    # usuario de amazon para Santiago y Nicolas
    # Columnas del ODS, de que archivos vienen... 

    get_data_from_talon_service >> validation_get_data_talon_service >> \
        [copy_data_from_lfs_to_hdfs_campaigns,copy_data_from_lfs_to_hdfs_coupons] >> validation_copy_data_from_lfs_to_hdfs >> \
            [process_data_and_move_to_s3_campaigns,process_data_and_move_to_s3_coupons] >> dwh_load_coupons_from_s3 >> dwh_generate_fact_talon_coupons

