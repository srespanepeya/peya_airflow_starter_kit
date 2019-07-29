from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.models import Variable
from airflow.contrib.operators.bigquery_check_operator import BigQueryValueCheckOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from urllib import request, parse
import json
import airflow.utils.helpers

# Parames del DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 27),
    'email': ['bigdata@pedidosya.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 15,
    'retry_delay': timedelta(minutes=60)
}

yesterday_date = datetime.strftime(datetime.now() - timedelta(1), "%Y%m%d")
table_name_android = '`110190327.ga_sessions_' + yesterday_date + '`'
table_name_ios = '`110191523.ga_sessions_' + yesterday_date + '`'
table_name_webd = '`110187139.ga_sessions_' + yesterday_date + '`'
table_name_webm = '`110187640.ga_sessions_' + yesterday_date + '`'

with DAG('BigData_BQSessions', schedule_interval='0 9 * * *', catchup=False, default_args=default_args) as dag:

    execution_start = DummyOperator(
        task_id='execution_start',
        dag=dag)

    check_ga_sessions_all_platforms_exist = BigQueryOperator(
        task_id='check_ga_sessions_all_platforms_exist',
        bql="""
            SELECT 1 FROM {{ params.table_name_android }}
            UNION ALL
            SELECT 1 FROM {{ params.table_name_ios }}
            UNION ALL
            SELECT 1 FROM {{ params.table_name_webd }}
            UNION ALL
            SELECT 1 FROM {{ params.table_name_webm }}
            """,
        use_legacy_sql=False,
        bigquery_conn_id='peya_bigquery',
        params={'table_name_android': table_name_android,
                'table_name_ios': table_name_ios,
                'table_name_webd': table_name_webd,
                'table_name_webm': table_name_webm}
    )

    execute_python_bq_sessions = SSHOperator(
        task_id="execute_python_bq_sessions",
        command="""
        /home/hduser/spark/apps/bigquery/bigquery.sh etl_digital_analytics_v2 Pedidosya_ga_sessions bpy---pedidosya bq_sessions false >> /home/hduser/spark/logs/extraccion_bq_sessions.log 2>&1
        """,
        timeout = 20,
        ssh_conn_id = "ssh_hadoop_namenode_bi"
    )

    execution_start >> check_ga_sessions_all_platforms_exist
    check_ga_sessions_all_platforms_exist >> execute_python_bq_sessions