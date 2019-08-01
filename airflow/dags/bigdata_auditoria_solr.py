from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta, date

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

with DAG('BigData_Auditoria_Solr', schedule_interval=None, catchup=False, default_args=default_args) as dag:
    execute_auditoria_solr = BashOperator(
    task_id='execute_auditoria_solr',
    bash_command="""
        /home/hduser/auditoria_nrt/run_auditoria_nrt.sh
        """
    )

    execute_auditoria_solr