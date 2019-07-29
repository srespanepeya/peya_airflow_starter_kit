# Se importan las librerias argparse y ConfigParser para recibir parametros en las funciones
import argparse, configparser
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

def has_column(df, col):
    try:
        df[col]
        return True
    except:
        return False

def add_missing_columns(df):
    for i in range(1, 201):
        col = 'cd{0}'.format(i)
        if not has_column(df,col):
            print('---@>Added column {0} in dataframe because it was missing.'.format(col))
            df = df.withColumn(col,lit(None).cast(StringType()))
    return df


def funcion_load_flat_sessions_to_hdfs(app_args):
    try:    
        if not app_args.fecha:
                dt = datetime.datetime.now()
                partition = dt.strftime("%Y%m%d")
        else:
                partition = app_args.fecha
        conf = SparkConf().setAppName("ETL_PROCESS_COUPONS_TO_S3_{0}".format(partition))
        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)
        print('--->START READING DATA FROM HDFS JSON')
        df = sqlContext.read.parquet('hdfs://hadoop-namenode-ti:9000/entidades/campaigns/'+partition+'/06/part-00000-5bcada87-6b35-43a5-8520-645466064f9e-c000.snappy.parquet')
        print('<---END READING DATA FROM HDFS JSON')
        #print('--->START DISCOVERING AND ADJUSTING SCHEMA')
        #df = df.withColumn('attributes', regexp_replace('attributes', "; ","\"\,\""))
        #df = df.withColumn('attributes', regexp_replace('attributes', ": ","\":\""))
        #df = df.withColumn('attributes', regexp_replace('attributes', "\{","\{\""))
        #df = df.withColumn('attributes', regexp_replace('attributes', "}","\"}"))
        df.printSchema()
        #att_fields = ['CountryId','CityId','DiscountAmount','UsedAmount','PeyaPays','Title','OrderId','Reason','AgentId','AdvocateId']
        #att_schema = sqlContext.read.json(df.rdd.map(lambda row: row.attributes)).schema
        #schema_fields = att_schema.fieldNames()

        #for f in att_fields:
        #        if not f in schema_fields:
        #                print(f,'---@>Adding column {0} because it was missing')
        #                att_schema.add(f, StringType(), True)  

        #df = df.withColumn('attributes', from_json('attributes', att_schema))

        #df.select("attributes").show(10,truncate = False)
        #df.printSchema()
        #print('<---END DISCOVERING AND ADJUSTING SCHEMA')
        #df.createOrReplaceTempView('coupons')
        #consulta = '''
        #        select id,
        #               campaignid as campaing_id,
        #               recipientintegrationid as user_id,
        #               cast(from_unixtime(unix_timestamp(replace(replace(created, "T"," "),"Z"," "), 'yyyy-MM-dd HH:mm:ss')) as timestamp) as created,
        #               cast(from_unixtime(unix_timestamp(replace(replace(startdate, "T"," "),"Z"," "), 'yyyy-MM-dd HH:mm:ss')) as timestamp) as start,
        #               cast(from_unixtime(unix_timestamp(replace(replace(expirydate, "T"," "),"Z"," "), 'yyyy-MM-dd HH:mm:ss')) as timestamp) as expiry,
        #               value as value,
        #               limitval as usage_limit,
        #               counter as usage_counter,
        #               cast(attributes as string) as attributes_json,
        #               attributes.CountryId as att_country,
        #               attributes.CityId as att_city,
        #               attributes.DiscountAmount as att_discount_amount,
        #               attributes.UsedAmount as att_used_amount,
        #               attributes.PeyaPays as att_peyapays,
        #               attributes.Title as att_title,
        #               attributes.OrderId as att_order,
        #               attributes.Reason as att_backoffice_reason,
        #               attributes.AgentId as att_backoffice_user,
        ##               attributes.AdvocateId as advocate_id,
        #               batchid as batch_id
        #        from coupons 
        #'''
        #df_coupons = sqlContext.sql(consulta)
        #df_coupons.printSchema()
        #df_coupons.show(10,truncate=False)
        #df_coupons.coalesce(1) \
        #          .write \
        #          .mode ("overwrite") \
        #          .format("com.databricks.spark.csv") \
        #          .option("header","true") \
        #          .option("codec", "org.apache.hadoop.io.compress.GzipCodec") \
        #          .save("s3a://peyabi.bigdata/talon/coupons/export")
    except:
        print(traceback.format_exc())
        time.sleep(1) #workaround para el bug del thread shutdown

def get_app_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("-f", "--fecha", help="fecha de la particion que se procesara en formato yyyymmdd")
        return parser.parse_args()

if __name__ == '__main__':
        app_args = get_app_args()
        funcion_load_flat_sessions_to_hdfs(app_args)
