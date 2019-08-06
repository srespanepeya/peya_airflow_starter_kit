# se importa la libreria traceback para manejo de errores y excepciones
import traceback
# se importa la libreria time para realiza un sleep mientras que spark finaliza la consolidacion de los archivos
import time
# Libreria para la generacion de las fechas
import datetime
# Se importa la libreria pyspark las funciones para declarar el contexto (SparkContext) y definir la configuracion (
# SparkConf)
from pyspark import SparkContext, SparkConf
# Se importa la libreria pyspark.sql la funciona para definir el contexto de una consulta (SQLContext)
from pyspark.sql import SQLContext
# Se importa la libreria pyspark.sql.types todas las funciones de los tipos de datos en las consultas
from pyspark.sql.types import *
import subprocess
# se importan las librerias os y sys para enviar comandos al sistema operativo.
import os, sys
import argparse, ConfigParser


def auditoria_talon_hdfs_to_s3():
	try:
		dt = datetime.datetime.now()
        	fecha = dt.strftime("%Y%m%d")
		conf = SparkConf().setAppName("auditoria_talon_hdfs_to_s3")
		sc = SparkContext(conf=conf)
		sqlContext = SQLContext(sc)
		entity = app_args.entity
		df_hdfs = sqlContext.read.parquet('hdfs://hadoop-namenode-ti:9000/entidades/'+entity+'/batch/*.parquet')
		entity_hdfs = df_hdfs.count()
		df_s3 = sqlContext.read.csv('s3a://peyabi.bigdata/talon/'+entity+'/export/*')
		entity_s3 = df_s3.count()
                print(entity.upper() + ' HDFS: ' + str(entity_hdfs))
		print(entity.upper() + ' S3: ' + str(entity_s3))
		if int((entity_s3) < int(entity_hdfs)):
                        print('ATENCION: Hay menos ' +entity+ ' en S3 vs su origen (HDFS)')
			exit(1)
                else:
                        print('SUCCESS: NO HAY DIFERENCIA DE ' + entity.upper())
	except Exception, e:
		print('Exception: ' + str(e))
		time.sleep(1)  # workaround para el bug del thread shutdown
		exit(1)

def get_app_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("-e", "--entity", help="endtidad coupons/campaigns")
        return parser.parse_args()

if __name__ == '__main__':
	app_args = get_app_args()
	auditoria_talon_hdfs_to_s3()

