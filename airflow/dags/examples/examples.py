#   ____________       _____________
#  ____    |__( )_________  __/__  /________      __
# ____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /
# ___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /
# __/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/

#Dummy operator
from airflow.operators.dummy_operator import DummyOperator
execution_end = DummyOperator(task_id='execution_end')


#SSH operator
from airflow.contrib.hooks import SSHHook
sshHook = SSHHook(conn_id=<YOUR CONNECTION ID FROM THE UI>)
run_ssh_command = SSHExecuteOperator(
    task_id="run_ssh_command",
    bash_command="some magic command",
    ssh_hook=sshHook)

#RedshiftToS3Transfer



#S3 to Redshift
from airflow.operators import PostgresOperator
from airflow.hooks import S3Hook
#Se crea un conector para S3
s3 = S3hook(aws_conn_id="aws_conection_id") 
#Se crea un postgress operator que ejecutara el script para llevar de S3 a redshift
redshift_load_task = PostgresOperator("""
    copy my_table 
    FROM '{{ params.source }}' 
    ACCESS_KEY_ID '{{ params.access_key}}' 
    SECRET_ACCESS_KEY '{{ params.secret_key }}' 
    REGION 'eu-west-1' 
    ACCEPTINVCHARS 
    IGNOREHEADER 1 
    FILLRECORD 
    CSV
    BLANKSASNULL 
    EMPTYASNULL 
    MAXERROR 100 
    DATEFORMAT 'MM/DD/YYYY'
""",
postgres_conn_id="redshift_conn_id",
database="database_name",
params={
    'source': 's3://my_bucket/my_file.csv',
    'access_key': s3.get_credentials().access_key,
    'secret_key': s3.get_credentials().secret_key,
},
)

#BashOperator
# Ejemplo que crea un directorio, y copia un archivo desde GCS a dicho dorectorio
from airflow.operators.bash_operator import BashOperator
create_tmp_folder = BashOperator(
    task_id='create_destination_folder',
    bash_command="""
        mkdir -p /tmp/test_folder
        cd /tmp/test_folder
        gsutil cp gs://my_bucket/my_file.csv /tmp/test_folder/
        """ 
)


#PythonOperator
def f_send_message_to_slack(text):
    post = {"text": "{0}".format(text)}

    try:
        json_data = json.dumps(post)
        req = request.Request("https://hooks.slack.com/services/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",
                              data=json_data.encode('ascii'),
                              headers={'Content-Type': 'application/json'})
        resp = request.urlopen(req)
    except Exception as em:
        print("EXCEPTION: " + str(em))

def f_call_send_message_to_slack(**kwargs):
    f_send_message_to_slack(kwargs['text'])

from airflow.operators.python_operator import PythonOperator
send_message_to_slack = PythonOperator(
    task_id='send_message_to_slack',
    provide_context=True,
    python_callable=f_call_send_message_to_slack,
    op_kwargs={'text': "Hola soy un texto de prueba"}
)    




#Run talend from airflow
    dwh_generate_fact_talon_coupons = SSHOperator(
        task_id="dwh_process_fact_talon_coupons",
        command="""
        /usr/bin/bash /home/peya/TALEND/TESTING/Vouchers/Fact/Prueba_Fact_Talon_Coupons/Prueba_Fact_Talon_Coupons_run.sh
        """,
        timeout = 20,
        ssh_conn_id = "ssh_talend_process_server"
    )

#BigQueryOperator




#PostgresOperator




# Las tareas se encadenan usando:
# task_a >> = task_b -> para definir que la tarea b empezara luego de que termine la tarea a
# task_b >> [task_c,task_d] -> para definir que las tareas c y d empezaran (y se ejecutaran en paralelo) luego de que termine la tarea b
# [task_c,task_d] >> task_e -> para definir que la tarea e comienza cuando las tareas c y d terminen