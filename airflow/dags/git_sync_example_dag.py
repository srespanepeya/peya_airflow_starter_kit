from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.models import Variable
import time

default_args = {
    'owner': 'root',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 14),
    'email': ['bi-producto@pedidosya.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 20,
    'retry_delay': timedelta(minutes=60)
}

# GET VARIABLES FROM AIRFLOW ENV
try:
    git_repo_path = string(Variable.get('my_git_path_variable'))
except:
    git_repo_path = "/my/path/to/my/git/repo"



###############################################################################################
#DAG Creation
with DAG('GIT_Sync_DAG', schedule_interval=None, catchup=False, default_args=default_args) as dag:
# with DAG('HUE_Data_Generation_DAG', schedule_interval=None, catchup=False, default_args=default_args) as dag:
    
    update_bi_airflow_git_repo = BashOperator(
        task_id='update_bi_airflow_git_repo',
        bash_command="""
            cd {0}
            pwd
            git stash
            git pull origin master
        """.format(git_repo_path)
    )

    # Set task downstream
    update_bi_airflow_git_repo 

