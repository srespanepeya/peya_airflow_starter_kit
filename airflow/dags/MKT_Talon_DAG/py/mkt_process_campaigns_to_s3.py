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

def funcion_process_mkt_campaigns(app_args):
    try:
        if not app_args.fecha:
                dt = datetime.datetime.now()
                partition = dt.strftime("%Y%m%d")
        else:
                partition = app_args.fecha
        conf = SparkConf().setAppName("ETL_PROCESS_COUPONS_TO_S3_{0}".format(partition))
        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)
        print('--->START READING CAMPAIGNS FROM HDFS')
        df = sqlContext.read.parquet('hdfs://hadoop-namenode-ti:9000/entidades/campaigns/batch/*.parquet')
        print('<---END READING CAMPAIGNS FROM HDFS')
        print('----@>ROW COUNT:{0}'.format(df.count()))
        print('--->START DISCOVERING AND ADJUSTING SCHEMA')
        df.printSchema()
        print('--->END DISCOVERING AND ADJUSTING SCHEMA')

        df.createOrReplaceTempView('campaigns')
        consulta = '''
           select id as id,
                  trim(name) as name,
                  trim(description) as description,
                  replace(replace(created, "T"," "),"Z","") as created,
                  replace(replace(updated, "T"," "),"Z","") as updated,
                  updatedBy as updated_by,
                  replace(replace(lastActivity, "T"," "),"Z","") as last_activity,
                  replace(replace(start, "T"," "),"Z","") as start_time,
                  replace(replace(end, "T"," "),"Z","") as end_time,
                  state as state,
                  couponRedemtionCount as coupon_redemtion_count,
                  refferalRedemtionCount as refferal_redemtion_count,
                  discountCount as discount_count

           from campaigns
        '''
        df_campaigns = sqlContext.sql(consulta)
        df_campaigns.show(10,truncate=False)
        df_campaigns.write \
                .mode ("overwrite") \
                .format("com.databricks.spark.csv") \
                .option("codec", "org.apache.hadoop.io.compress.GzipCodec") \
                .save("s3a://peyabi.bigdata/talon/campaigns/export")        

    except:
        print(traceback.format_exc())
        time.sleep(1)
        exit(1)

def get_app_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("-f", "--fecha", help="fecha de la particion que se procesara en formato yyyymmdd")
        return parser.parse_args()

if __name__ == '__main__':
        app_args = get_app_args()
        funcion_process_mkt_campaigns(app_args)
