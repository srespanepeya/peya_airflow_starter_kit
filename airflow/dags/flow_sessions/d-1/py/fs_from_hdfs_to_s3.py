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
from datetime import date, timedelta

def move_data(app_args):
    try:
        if not app_args.partition:
            yesterday = date.today() + timedelta(days=-1)
            partition = yesterday.strftime("%Y%m%d")
        else:
            partition = app_args.partition
        conf = SparkConf().setAppName("ETL_BATCH_FLOW_SESSIONS_{0}".format(partition))
        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)
        print('--->START READING FS DATA FROM HDFS')
        df1 = sqlContext.read.parquet('hdfs://hadoop-namenode-bi:9000/alan/ods/flow_sessions/{0}/*'.format(partition))
        df2 = sqlContext.read.parquet('hdfs://hadoop-namenode-bi:9000/alan/ods/flow_session_events/{0}/*'.format(partition))
        df3 = sqlContext.read.parquet('hdfs://hadoop-namenode-bi:9000/alan/ods/flow_sessions_chats/{0}/*'.format(partition))

        print('--->START WRITING DATA ON S3')
        df1.write.mode ("overwrite") \
                .format("com.databricks.spark.csv") \
                .option("encoding", "UTF-8") \
                .option("codec", "org.apache.hadoop.io.compress.GzipCodec") \
                .save("s3a://peyabi.bigdata/flow_sessions/flow_sessions/{0}".format(partition))
        df2.write.mode ("overwrite") \
                .format("com.databricks.spark.csv") \
                .option("encoding", "UTF-8") \
                .option("codec", "org.apache.hadoop.io.compress.GzipCodec") \
                .save("s3a://peyabi.bigdata/flow_sessions/flow_session_events/{0}".format(partition))
        df3.write.mode ("overwrite") \
                .format("com.databricks.spark.csv") \
                .option("encoding", "UTF-8") \
                .option("codec", "org.apache.hadoop.io.compress.GzipCodec") \
                .save("s3a://peyabi.bigdata/flow_sessions/flow_sessions_chats/{0}".format(partition))

        print('<---END WRITING DATA ON S3')
    except:
        print(traceback.format_exc())
        time.sleep(1) #workaround para el bug del thread shutdown
        raise

def get_app_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("-p", "--partition", help="fecha")
        return parser.parse_args()

if __name__ == '__main__':
        app_args = get_app_args()
        move_data(app_args)
