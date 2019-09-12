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

#casteo la variable obtenida de las Variables generales de airflow para obtener la ruta del repo    
try:
    git_path=Variable.get('git_bi_bigdata_path')
except:
    git_path='/root/airflow_extra/bigdata-airflow'

dag_path="{0}/airflow/dags/flow_sessions/d-1".format(git_path)
py_path= "{0}/py".format(dag_path)


# Variables
try:
    API_HOST = Variable.get('bi_api_service_host')
    API_PORT = Variable.get('bi_api_service_port')
except:
    # En caso de fallos, seteamos valores por defecto
    API_HOST = "10.0.91.124"
    API_PORT = "9003"

PROTOCOLO = "http://"
API_ENDPOINT = "{0}:{1}".format(API_HOST, API_PORT)   

today = datetime.datetime.now()
fecha = today.strftime("%Y%m%d")

def validateFs(**kwargs):
    # definimos request
    API_REQUEST = "{0}{1}/api/hive/validate/flow_sessions?tableName=flow_sessions&date={2}".format(PROTOCOLO, API_ENDPOINT, fecha)
    print(API_REQUEST)
    # enviamos post request
    r = requests.get(url = API_REQUEST)
    return r

def validateFs(**kwargs):
    API_REQUEST = "{0}{1}/api/hive/validate/flow_sessions?tableName=flow_sessions_event&date={2}".format(PROTOCOLO, API_ENDPOINT, fecha)
    print(API_REQUEST)
    # enviamos post request
    r = requests.get(url = API_REQUEST)
    return r

def validateFs(**kwargs):
    # definimos request
    API_REQUEST = "{0}{1}/api/hive/validate/flow_sessions?tableName=flow_sessions_chat&date={2}".format(PROTOCOLO, API_ENDPOINT, fecha)
    print(API_REQUEST)
    # enviamos post request
    r = requests.get(url = API_REQUEST)
    return r

def should_run(**kwargs):
    # definimos request
    fs_equal = validateFs()
    fse_equal = validateFse()
    fsc_equal = validateFsc()
    if fs_equal and fse_equal and fsc_equal:
        return "dummy"
    else:
        return "dia"

#aca debemos llamar al servicio de diego para ver si cierra alan vs ods dia anterior
cond = BranchPythonOperator(
    task_id='condition',
    provide_context=True,
    python_callable=should_run,
    dag=dag,
)

#cargar fecha de ayer
yesterday = date.today() + timedelta(days=-1)
ayer = yesterday.strftime("%Y%m%d")

with DAG('BigData_Flow_Session_Related_Hdfs_to_S3', schedule_interval="0 7 * * 1-7", catchup=False, default_args=default_args) as dag:
    
    dia = SSHOperator(
        task_id="session_alan_to_ods",
        command="""
        /usr/bin/bash /home/hduser/hive/scripts/create_table_alan_flow_sessions_ayer.sh
        /usr/bin/bash /home/hduser/hive/scripts/create_table_alan_flow_sessions_events_ayer.sh
        /usr/bin/bash /home/hduser/hive/scripts/create_table_alan_flow_sessions_chats_ayer.sh
        """,
        timeout = 60,
        ssh_conn_id = "ssh_hadoop_resmanager_bi",
        dag = dag
    )

    dummy = DummyOperator(task_id='dummy', dag=dag)
    
    s3 = BashOperator(
        task_id='process_data_and_move_to_s3_fs',
        bash_command="""
        echo "--->Begin BATCH Flow sessions"
        chmod 755 {0}/fs_from_hdfs_to_s3.py
        /home/hduser/spark/bin/spark-submit --master spark://hadoop-namenode-bi:7077 --driver-memory 10G --driver-cores 8 --executor-memory 10G --conf spark.cores.max=8 {0}/fs_from_hdfs_to_s3.py -p "{1}"
        """.format(py_path, ayer),
        dag=dag
)

cond >> [dia,dummy] >> s3
   
