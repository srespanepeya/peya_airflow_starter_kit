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
import json
import urllib2
import argparse, ConfigParser


def auditoria_talon_s3_to_imports_ods_redshift():
	try:
		dt = datetime.datetime.now()
        	fecha = dt.strftime("%Y%m%d")
		conf = SparkConf().setAppName("auditoria_talon_s3_to_imports_ods_redshift")
		sc = SparkContext(conf=conf)
		sqlContext = SQLContext(sc)
		entity = app_args.entity
		df = sqlContext.read.csv('s3a://peyabi.bigdata/talon/'+entity+'/export/*')
		entity_s3 = df.count()
		print(entity + ' S3: ' + str(entity_s3))
		if entity == 'coupons':
			response_entity = json.load(urllib2.urlopen("http://10.0.91.124:9003/api/count/redshift/prueba_data_ods_talon_vouchers/imports"))
		else:
			response_entity = json.load(urllib2.urlopen("http://10.0.91.124:9003/api/count/redshift/dim_talon_campaigns/public"))
                cantidad_registros_entity_talon = response_entity['cantRegistros']
		print(entity + ' REDSHIFT: ' + str(cantidad_registros_entity_talon))
		if int((cantidad_registros_entity_talon) < int(entity_s3)):
                        print('ATENCION: Hay menos ' +entity+ ' en REDSHHIFT vs su origen (S3)')
			exit(1)
                else:
                        print('SUCCESS: NO HAY DIFERENCIA DE ' + entity)
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
	auditoria_talon_s3_to_imports_ods_redshift()

