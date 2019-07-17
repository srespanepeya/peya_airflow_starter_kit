from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.models import Variable
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.contrib.operators.mysql_to_gcs import mysql_to_gcs

default_args = {
    'owner': 'root',
    'depends_on_past': False,
    'start_date': datetime(2017, 7, 17),
    'email': ['marquicio.pagola@pedidosya.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sql': '"select id, name, short_name, culture from peyadb.country T'
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
  
def format_hello(**kwargs):
    return 'Extracting data from MySQL !! Current execution time is ' + kwargs['execution_date'].strftime('%Y-%m-%d')


with DAG('Moving-MySQL-Datalake-dag-MP', schedule_interval=None, catchup=False, default_args=default_args) as dag:

    generate_file_in_bucket = BigQueryToCloudStorageOperator(
        task_id='generate_file_in_bucket',
        source_project_dataset_table='%s{{ yesterday_ds_nodash }}' % (temp_table_name),
        destination_cloud_storage_uris=[
            'gs://peya_hue_generated_data/pedidosya_ga_sessions_{{ yesterday_ds_nodash }}_*.avro'
        ],
        export_format='AVRO',
        compression='SNAPPY',
        bigquery_conn_id='peya_bigquery'
    )

    # Slack Operators
    slack_success = SlackAPIPostOperator(
        dag=dag,
        task_id='slack-success',
        channel=Variable.get('slack_channel'),
        icon_url=Variable.get('slack_icon_url'),
        token=Variable.get('slack_token'),
        text=':muscle: - {time} - {dag} has completed'.format(
            dag='dag --> {}'.format('test'),
            time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        ),
        retries=0,
        email_on_failure='marquicio.pagola@pedidosya.com'
    )

    slack_error = SlackAPIPostOperator(
        dag=dag,
        task_id='slack-error',
        trigger_rule=TriggerRule.ONE_FAILED,
        channel=Variable.get('slack_channel'),
        icon_url=Variable.get('slack_icon_url'),
        token=Variable.get('slack_token'),
        text=':advertencia: - {time} - {dag} has failed!'.format(
            dag='dag --> {}'.format('Featured_Products_Daily_Report_DAG'),
            time=datetime.strftime(datetime.now() - timedelta(hours=3), '%Y-%m-%d %H:%M:%S'),
        ),
        email_on_failure='marquicio.pagola@pedidosya.com',
        retries=0
    )

    t1 >> t2