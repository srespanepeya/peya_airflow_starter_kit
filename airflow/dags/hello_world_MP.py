from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.models import Variable
from airflow.operators.MySqlOperator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
#from airflow.operators.mysql_to_hive import MySqlToHiveTransfer

default_args = {
    'owner': 'root',
    'depends_on_past': False,
    'start_date': datetime(2017, 7, 17),
    'email': ['marquicio.pagola@pedidosya.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

    # new code
class ReturningMySqlOperator(MySqlOperator):
    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id,
                         schema=self.database)
        return hook.get_records(
            self.sql,
            parameters=self.parameters)

def format_hello(**kwargs):
    return 'Extracting data from MySQL !! Current execution time is ' + kwargs['execution_date'].strftime('%Y-%m-%d')


with DAG('Moving-MySQL-Datalake-dag-MP', schedule_interval=None, catchup=False, default_args=default_args) as dag:

    # new code
    t1 = ReturningMySqlOperator(
        task_id='basic_mysql',
        mysql_conn_id='local_mysql',
        sql="select id, name, short_name, culture from peyadb.country T",
        dag=dag)

    def get_records(**kwargs):
        ti = kwargs['ti']
        xcom = ti.xcom_pull(task_ids='basic_mysql')
        string_to_print = 'Value in xcom is: {}'.format(xcom)
        # Get data in your logs
        logging.info(string_to_print)

    t2 = PythonOperator(
        task_id='records',
        provide_context=True,
        python_callable=get_records,
        dag=dag)
    # end - new code

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

    # mybash_task = BashOperator(
    #     task_id='create_folder',
    #     bash_command="""
    #         mkdir -p %s
    #         """ %s ('MP_Folder')
    # )

    #connection id: local_mysql
    #mysql_task = MySqlOperator(
    #    conn_id='local_mysql',
    #    task_id='mysql_task',
    #    sql="select * from payments.discount_campaign T",
    #    dag=dag
    #)

    # mysql_task = MySqlToHiveTransfer(
    #     task_id='mysql_task',
    #     mysql_conn_id='local_mysql',
    #     hive_cli_conn_id='beeline_default',
    #     sql="select * from payments.discount_campaign T",
    #     hive_table='test_mysql_to_hive',
    #     recreate=True,
    #     delimiter=",",
    #     dag=dag
    # )
    # t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    # Define the DAG structure.
#     t1 >> t2 >> t3
    # mybash_task >> mysql_task
    # mysql_task >> [slack_success,slack_error]

    t1 >> t2