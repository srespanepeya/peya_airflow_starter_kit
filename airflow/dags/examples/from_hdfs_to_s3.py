# Se importan las librerias argparse y ConfigParser para recibir parametros en las funciones
import argparse, ConfigParser
# se importan las librerias os y sys para enviar comandos al sistema operativo.
import os, sys
# se importa la libreria traceback para manejo de errores y excepciones
import traceback
# se importa la libreria time para realiza un sleep mientras que spark finaliza la consolidacion de los archivos
import time
# Libreria para la generacion de las fechas
import datetime
# Se importa la libreria pyspark las funciones para declarar el contexto (SparkContext) y definir la configuracion (SparkConf)
from pyspark import SparkContext, SparkConf
# Se importa la libreria pyspark.sql la funciona para definir el contexto de una consulta (SQLContext)
from pyspark.sql import SQLContext
# Se importa la libreria pyspark.sql.types todas las funciones de los tipos de datos en las consultas
from pyspark.sql.types import *
# Se importa la libreria pandas
import pandas as pd
# Se importa la libreria de funciones de Spark SQL
from pyspark.sql.functions import *
import pyspark.sql.functions as f

def move_data(app_args):
    try:
        if not app_args.origen:
            raise
        else:
                partition = app_args.fecha
        conf = SparkConf().setAppName("ETL_BATCH_MKT_COUPONS_{0}".format(partition))
        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)
        print('--->START READING COUPONS DATA FROM HDFS')
        df = sqlContext.read.parquet('hdfs://hadoop-namenode-ti:9000/entidades/coupons/batch/*.parquet')
        print('<---END READING COUPONS DATA FROM HDFS')
        print('----@>ROW COUNT:{0}'.format(df.count()))
        df.printSchema()
        print('<---END DISCOVERING AND ADJUSTING SCHEMA')
        
        print('--->START WRITING DATA ON S3')
        df.write.mode ("overwrite") \
                .format("com.databricks.spark.csv") \
                .option("encoding", "UTF-8") \
                .option("codec", "org.apache.hadoop.io.compress.GzipCodec") \
                .save("s3a://peyabi.bigdata/talon/coupons/export")
        print('<---END WRITING DATA ON S3')
    except:
        print(traceback.format_exc())
        time.sleep(1) #workaround para el bug del thread shutdown
        raise
        
        

def get_app_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("-o", "--origen", help="ruta al hdfs de origen")
        parser.add_argument("-d", "--destino", help="ruta al s3 de destino")
        return parser.parse_args()

if __name__ == '__main__':
        app_args = get_app_args()
        move_data(app_args)

####################################################DAG##########################################################
try:
    git_repo_path = Variable.get('git_bi_marketing_path')
except:
    git_repo_path = "/root/airflow_extra/bi-marketing"

dag_path="{0}/airflow/dag/MKT_Talon_DAG".format(git_repo_path)
py_path= "{0}/py".format(dag_path)


process_data_and_move_to_s3_coupons = BashOperator(
        task_id='process_data_and_move_to_s3_coupons',
        bash_command="""
        echo "--->Begin BATCH MKT Coupons"
        chmod 755 {0}/mkt_process_coupons_to_s3.py
        /home/hduser/spark/bin/spark-submit --master spark://hadoop-namenode-bi:7077 --driver-memory 10G --driver-cores 8 --executor-memory 10G --conf spark.cores.max=8 {0}/mkt_process_coupons_to_s3.py -o "{1}" -d "{2}"
        """.format(py_path,ruta_origen,ruta_destino)
)