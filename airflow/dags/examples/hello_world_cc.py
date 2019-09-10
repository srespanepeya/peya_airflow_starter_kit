from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.models import Variable

default_args = {
    'owner': 'root',
    'depends_on_past': False,
    'start_date': datetime(2017, 7, 17),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


def format_hello(**kwargs):
    return 'Hello from Python !! Current execution time is ' + kwargs['execution_date'].strftime('%Y-%m-%d')


with DAG('hello-world-dag-cc', schedule_interval=None, catchup=False, default_args=default_args) as dag:

    """ slack_success = SlackAPIPostOperator(
        dag=dag,
        task_id='slack-success',
        channel=Variable.get('slack_channel'),
        icon_url=Variable.get('slack_icon_url'),
        token=Variable.get('slack_token'),
        text=':doughnut: - {time} - {dag} has completed'.format(
            dag='dag --> {}'.format('test'),
            time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        ),
        retries=0,
        email_on_failure='bi-producto@pedidosya.com'
    ) """

    
    # Define the DAG structure.
#     t1 >> t2 >> t3
