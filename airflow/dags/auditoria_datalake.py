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

with DAG('Auditoria_DL', schedule_interval='0 10 * * *', catchup=False, default_args=default_args) as dag:
    execute_auditoria = BashOperator(
    task_id='execute_auditoria',
    bash_command="""
        /home/hduser/auditoria/run_auditoria_dl.sh
        """
    )

    execute_auditoria

