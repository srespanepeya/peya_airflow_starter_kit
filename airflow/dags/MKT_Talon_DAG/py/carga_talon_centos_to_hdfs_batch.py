#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
#se importan las librerias argparse y ConfigParser para recibir parametros en las funciones
import argparse, ConfigParser
# se importan las librerias os y sys para enviar comandos al sistema operativo.
import os, sys
# se importa la libreria traceback para manejo de errores y excepciones
import traceback
# se importa la libreria time para realiza un sleep mientras que spark finaliza
from time import time
from pyspark import SparkContext
from pyspark import SparkConf
# Se importa la libreria pyspark.sql la funcion para definir el contexto de una consulta (SQLContext)
from pyspark.sql import SQLContext
# Se importa la libreria pyspark.sql.types todas las funciones de los tipos de datos en las consultas
from pyspark.sql.types import *
# Se importa la libreria pyspark.sql la funcion Row
from pyspark.sql import Row
# Se importa la libreria Process para el multiplocresamiento
from multiprocessing import Process, Queue
# Se importa la libreria request para realizar la invocacion al url de solr
import requests
# Se importa la librerira json para procesar el mensaje de respuesta de solr
import json


def funcion_centos_hdfs(app_args):
        try:
                dir_base_talon=app_args.ruta
                dir_entidad=dir_base_talon+app_args.entidad
                print("dir_base_talon="+dir_base_talon)
                if app_args.entidad == "coupons":
                    dir_entidad=dir_base_talon+"CouponsCsv"
                    print("dir_entidad="+dir_entidad)
                else:
                    dir_entidad=dir_base_talon+"Campaigns"
                    print("dir_entidad="+dir_entidad)
                dt = datetime.datetime.now()
                fecha = dt.strftime("%Y%m%d")

			    # asigna a la variable con el nombre de la aplicacion que se ejecutara en spark
                conf =  SparkConf().setAppName(app_args.entidad.lower()+"_from_carga_inicial_centos_to_hdfs_batch")
                # se crea la variable sc que define el contexto de ejecucion con el parametro de configuracion
                sc = SparkContext(conf=conf)
                # se crea la variable sqlContext para realizar la consulta
                sqlContext = SQLContext(sc)
		
			    if app_args.tipo_archivo == 'csv':
                        #print("sed -i "+"\'"+"s/"+", \"\""+"/; \"\"/g"+"\' "+dir_entidad+"/"+fecha+"/batch/*.csv")
                        #os.system("sed -i "+"\'"+"s/"+"\"\""+"//g"+"\' "+dir_entidad+"/"+fecha+"/batch/*.csv")
                        #print("sed -i "+"\'"+"s/"+"\"\""+"//g"+"\' "+dir_entidad+"/"+fecha+"/batch/*.csv")
                        #df = sqlContext.read.option("header", "true").option("delimiter", ",").option("quote", "\"").csv("file://"+dir_entidad+"/"+fecha+"/batch/*.csv")
                        #df = sqlContext.read.option("header", "true").option("delimiter", ",").csv("file://"+dir_entidad+"/20190808/batch/97.csv.out")
                        #df.printSchema()
                        #df.show(10,truncate=False)
				        os.system("/usr/bin/bash /home/hduser/spark/apps/runawk.sh {0}/{1}/batch/*.csv".format(dir_entrada,fecha))
                        #df = sqlContext.read.option("header", "true").option("delimiter", ",").option("quote", "\"").csv("file://"+dir_entidad+"/"+fecha+"/batch/*.csv")
                        df = sqlContext.read.option("header", "true").option("delimiter", ",").csv("file://"+dir_entidad+"/"+fecha+"/batch/*.out")

				        
						#df = sqlContext.read.option("header", "true").option("delimiter", ",").option("quote", "\"").csv("file:///home/hduser/hdfs/data/solr/"+app_args.ruta+"/"+partition+"/*.csv")
			    else:
                        if app_args.tipo_archivo == 'json':
                                df = sqlContext.read.json("file://"+dir_entidad+"/"+fecha+"/batch/*.json")
                        else:
                                print("error con el tipo de archivo")

    				
                #df.printSchema()
                #df.show()
    			#print("cantidad de registros:"+str(df.count()))
				df.write.parquet('hdfs://hadoop-namenode-ti:9000/entidades/'+app_args.entidad.lower()+'/batch/', mode='overwrite', compression='snappy')
                #escribe en el hdfs en parquet y comprimido en snapy y segun la particion
    	except:
                print traceback.format_exc()
				exit(1)

def get_app_args():
    parser = argparse.ArgumentParser()
	parser.add_argument("-o", "--hora", help="hora de la extraccion en formato HH, ejemplo 06, esto es por defecto")
	parser.add_argument("-p", "--particion", help="fecha de la particion en formato yyyymmdd, ejemplo 2018-11-15, si no se indica se toma la fecha actual")
	parser.add_argument("-e", "--entidad", help="nombre de la entidad a procesar")
	parser.add_argument("-r", "--ruta", help="ruta a partir de /home/hduser/hdfs/data/solr para la lectura del archivo")
    parser.add_argument("-t", "--tipo_archivo", help="tipo archivo ejemplo:csv o json")
	return parser.parse_args()

if __name__ == '__main__':
    app_args = get_app_args()
    funcion_centos_hdfs(app_args)
