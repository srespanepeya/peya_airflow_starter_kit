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


def auditoria_talon_s3_to_imports_ods_redshift():
	try:
		dt = datetime.datetime.now()
        	fecha = dt.strftime("%Y%m%d")
		conf = SparkConf().setAppName("auditoria_talon_s3_to_imports_ods_redshift")
		sc = SparkContext(conf=conf)
		sqlContext = SQLContext(sc)
		df_coupons_s3 = sqlContext.read.csv('s3a://peyabi.bigdata/talon/coupons/export/*')
		df_campaigns_s3 = sqlContext.read.csv('s3a://peyabi.bigdata/talon/campaigns/export/*')
		coupons_s3 = df_coupons_s3.count()
		campaigns_s3 = df_campaigns_s3.count()
		print('COUPONS S3: ' + str(coupons_s3))
		print('CAMPAIGNS S3: ' + str(campaigns_s3))
		response_coupons = json.load(urllib2.urlopen("http://10.0.91.124:9003/api/count/redshift/prueba_data_ods_talon_vouchers/imports"))
                cantidad_registros_coupons_talon = response_coupons['cantRegistros']
		response_campaigns = json.load(urllib2.urlopen("http://10.0.91.124:9003/api/count/redshift/dim_talon_campaigns/public"))
		cantidad_registros_dim_talon_campaigns = response_campaigns['cantRegistros']
		print('COUPONS REDSHIFT[SCHEMA: IMPORTS, TABLE: DATA_ODS_TALON_VOUCHERS] :: ' + str(cantidad_registros_coupons_talon))
		if int((cantidad_registros_coupons_talon) < int(coupons_s3)):
                        print('ATENCION: Hay menos cupones en REDSHHIFT vs su origen (S3)')
			exit(1)
                else:
                        print('SUCCESS: NO HAY DIFERENCIA DE COUPONS')
                if (int(cantidad_registros_dim_talon_campaigns) < int(campaigns_s3)):
                        print('ATENCION: Hay menos campaigns en REDSHIFT vs su origen (S3)')
			exit(1)
                else:
                        print('SUCCESS: NO HAY DIFERENCIA DE CAMPAIGNS')
	except Exception, e:
		print('Exception: ' + str(e))
		time.sleep(1)  # workaround para el bug del thread shutdown
		exit(1)

if __name__ == '__main__':
	auditoria_talon_s3_to_imports_ods_redshift()

