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

def funcion_load_flat_sessions_to_hdfs(app_args):
    try:
        if not app_args.fecha:
                dt = datetime.datetime.now()
                partition = dt.strftime("%Y%m%d")
        else:
                partition = app_args.fecha
        conf = SparkConf().setAppName("ETL_BATCH_MKT_COUPONS_{0}".format(partition))
        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)
        print('--->START READING COUPONS DATA FROM HDFS')
        df = sqlContext.read.parquet('hdfs://hadoop-namenode-ti:9000/entidades/coupons/batch/*.parquet')
        print('<---END READING COUPONS DATA FROM HDFS')
        print('----@>ROW COUNT:{0}'.format(df.count()))
        print('--->START DISCOVERING AND ADJUSTING SCHEMA')
        df = df.withColumn('attributes', regexp_replace('attributes', "; ","\"\,\""))
        df = df.withColumn('attributes', regexp_replace('attributes', ": ","\":\""))
        df = df.withColumn('attributes', regexp_replace('attributes', "\{","\{\""))
        df = df.withColumn('attributes', regexp_replace('attributes', "}","\"}"))
        df.printSchema()
        att_fields = ['CountryId','CityId','DiscountAmount','UsedAmount','PeyaPays','Title','OrderId','Reason','AgentId','AdvocateId','OriginalId']
        att_schema = sqlContext.read.json(df.rdd.map(lambda row: row.attributes)).schema
        schema_fields = att_schema.fieldNames()

        for f in att_fields:
                if not f in schema_fields:
                        print(f,'---@>Adding column {0} because it was missing')
                        att_schema.add(f, StringType(), True)

        df = df.withColumn('attributes', from_json('attributes', att_schema))

        #df.select("attributes").show(10,truncate = False)
        df.printSchema()
        print('<---END DISCOVERING AND ADJUSTING SCHEMA')
        
        df.createOrReplaceTempView('coupons')
        consulta = '''
                select id,
                       campaignid as campaing_id,
                       recipientintegrationid as user_id,
                       replace(replace(created, "T"," "),"Z","") as created,
                       replace(replace(startdate, "T"," "),"Z","") as start,
                       replace(replace(expirydate, "T"," "),"Z","") as expiry,
                       value as value,
                       limitval as usage_limit,
                       counter as usage_counter,
                       attributes.CountryId as att_country,
                       attributes.CityId as att_city,
                       attributes.DiscountAmount as att_discount_amount,
                       attributes.UsedAmount as att_used_amount,
                       attributes.PeyaPays as att_peyapays,
                       attributes.Title as att_title,
                       attributes.OrderId as att_order,
                       attributes.Reason as att_backoffice_reason,
                       attributes.AgentId as att_backoffice_user,
                       batchid as batch_id,
                       attributes.AdvocateId as advocate_id,
                       attributes.OriginalId as original_id
                from coupons
        '''
        df_coupons = sqlContext.sql(consulta)
        df_coupons.printSchema()
        df_coupons.show(10,truncate=False)
        #df_coupons.coalesce(1) \
        df_coupons.write \
                  .mode ("overwrite") \
                  .format("com.databricks.spark.csv") \
                  .option("codec", "org.apache.hadoop.io.compress.GzipCodec") \
                  .save("s3a://peyabi.bigdata/talon/coupons/export")
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