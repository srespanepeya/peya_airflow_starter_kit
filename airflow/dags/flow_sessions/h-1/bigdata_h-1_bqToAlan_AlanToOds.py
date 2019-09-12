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
    'owner': 'airflow',
    'depends_on_past': False,
    'wait_for_downstream': False,
    'start_date': datetime(2019, 7, 9),
    'email': ['diego.pietruszka@pedidosya.com','carlos.cristoforone@pedidosya.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=5)
}

today = datetime.datetime.now()
fecha = today.strftime("%Y%m%d")

with DAG('BigData_Flow_Session_Related_BigQuery_To_Ods', schedule_interval="0 */1 * * 1-7", catchup=False, default_args=default_args) as dag:
    
    bq_hdfs_fse = SSHOperator(
        task_id="flow_session_event_domi_to_alan",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/alan/alan.sh alan_hc_domicilios_prod flow_session_events dhh---global-service-alan alan
        /usr/bin/bash /home/hduser/spark/apps/alan/alan.sh alan_hc_pedidosya_prod flow_session_events dhh---global-service-alan alan  
        /usr/bin/bash /home/hduser/spark/apps/alan/audit_alan_from_bq_to_hdfs.sh flow_session_events event_start  
        """,
        timeout = 60,
        ssh_conn_id = "ssh_hadoop_namenode_bi",
        dag = dag
    )    

    bq_hdfs_fs = SSHOperator(
         task_id="flow_session_domi_to_alan",
        command="""
        /usr/bin/bash /home/hduser/spark/apps/alan/alan.sh alan_hc_domicilios_prod flow_sessions dhh---global-service-alan alan
        /usr/bin/bash /home/hduser/spark/apps/alan/alan.sh alan_hc_pedidosya_prod flow_sessions dhh---global-service-alan alan
        /usr/bin/bash /home/hduser/spark/apps/alan/audit_alan_from_bq_to_hdfs.sh flow_sessions flow_session_start
        """,_
        timeout = 60,
        ssh_conn_id = "ssh_hadoop_namenode_bi",
        dag = dag
    )
    
    alan_ods_fs = SSHOperator(
        task_id="flow_session_hdfs_to_ods",
        command="""
        /usr/bin/bash /home/hduser/hive/scripts/create_table_alan_flow_sessions_peya.sh
        /usr/bin/bash /home/hduser/hive/scripts/validate_flow_sessions.sh flow_sessions {0}
        """.format(fecha),
        timeout = 60,
        ssh_conn_id = "ssh_hadoop_resmanager_bi",
        dag = dag
    )
  
    alan_ods_fse = SSHOperator(
        task_id="flow_session_event_hdfs_to_ods",
        command="""
        /usr/bin/bash /home/hduser/hive/scripts/create_table_alan_flow_sessions_event_peya.sh
        /usr/bin/bash /home/hduser/hive/scripts/validate_flow_sessions.sh flow_sessions_event {0}
        """.format(fecha),
        timeout = 60,
        ssh_conn_id = "ssh_hadoop_resmanager_bi",
        dag = dag
    )
  
    alan_ods_fsc = SSHOperator(
        task_id="flow_session_chat_hdfs_to_ods",
        command="""
        /usr/bin/bash /home/hduser/hive/scripts/create_table_alan_flow_sessions_chats.sh
        /usr/bin/bash /home/hduser/hive/scripts/validate_flow_sessions.sh flow_sessions_chat {0} 
        """.format(fecha),
        timeout = 60,
        ssh_conn_id = "ssh_hadoop_resmanager_bi",
        dag = dag
    )
 
bq_hdfs_fse >> bq_hdfs_fs >> alan_ods_fs >> alan_ods_fse >> alan_ods_fsc
   
