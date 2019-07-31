# from datetime import datetime, timedelta, date
# import airflow
# from airflow.models import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.hooks.S3_hook import S3Hook
# import logging
# from airflow.exceptions import AirflowException

# # Params DAG
# default_args = {
#     'owner': 'srespane',
#     'depends_on_past': False,
#     'start_date': datetime(2019, 7, 25),
#     'email': ['santiago.respane@pedidosya.com'],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 0,
#     'retry_delay': timedelta(minutes=5)
# }


# def moveCopyFilesBetweenS3Buckets(source_aws_conn_id, dest_aws_conn_id, source_s3_key,dest_s3_key,replace_file):
#         source_s3 = S3Hook(s3_conn_id=source_aws_conn_id)
#         dest_s3 = S3Hook(s3_conn_id=dest_aws_conn_id)
#         logging.info("Downloading source S3 file %s", source_s3_key)
#         if not source_s3.check_for_key(source_s3_key):
#             raise AirflowException("The source key {0} does not exist".format(source_s3_key))
#         source_s3_key_object = source_s3.get_key(source_s3_key)
#         dest_s3.load_string(
#             string_data=source_s3_key_object.get_contents_as_string(),
#             key=dest_s3_key,
#             replace=replace_file
#         )
#         logging.info("Copy successful")
#         source_s3.connection.close()
#         dest_s3.connection.close()

# def f_moveCopyFilesBetweenS3Buckets(**kwargs):
#     moveCopyFilesBetweenS3Buckets(kwargs['source_aws_conn_id'],kwargs['dest_aws_conn_id'],kwargs['source_s3_key'],kwargs['dest_s3_key'],kwargs['replace_file'])

# with DAG('QA_Operator_Tester_DAG', schedule_interval=None, catchup=False, default_args=default_args) as dag:

#     doTheThing = PythonOperator(
#         task_id='doTheThing',
#         provide_context=True,
#         python_callable=f_moveCopyFilesBetweenS3Buckets,
#         op_kwargs={
#             'source_aws_conn_id': "aws_s3_conn",
#             'dest_aws_conn_id': "aws_s3_peya_bi_ods_exports",
#             'source_s3_key': "s3a://peyabi.bigdata/talon/coupons/export/prueba_talon_vouchers.csv.gz",
#             'dest_s3_key': "s3a://peyabi.ods.exports/ods_vouchers/prueba_talon_vouchers.csv.gz",
#             'replace_file': True
#             }
#     )


