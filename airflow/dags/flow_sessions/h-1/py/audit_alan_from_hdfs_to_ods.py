import datetime
from datetime import date, timedelta
#se importan las librerias argparse y ConfigParser para recibir parametros en las funciones
import argparse, ConfigParser
# se importan las librerias os y sys para enviar comandos al sistema operativo.
import os, sys
# se importa la libreria traceback para manejo de errores y excepciones
import traceback
# se importa la libreria time para realiza un sleep mientras que spark finaliza
from time import time
import json
import urllib2
import urllib

def audit_alan_from_bq_to_hdfs(app_args):
    try:
        if not app_args.fecha:
            today = datetime.datetime.now()
            #print(today)
            fecha = today.strftime("%Y%m%d")
        else:
            fecha= app_args.fecha

        #GET AUDIT FILE
        #fq = urllib.quote('filesystem_folder:('+fq_writers+') AND filesystem_folder:('+fq_hora_minutos+')')
        tableNameOrigen = app_args.tabla
        
        req1= "http://10.0.91.124:9003/api/hive/validate/flow_sessions?tableName={0}&date={1}".format(tableNameOrigen,fecha)
        request1 = json.load(urllib2.urlopen(req1))
        
        if request1:
            print('VALIDACION OK!')
        else:
            print('VALIDACION NOK!')
            exit(1)
    except:
        print traceback.format_exc()

def get_app_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("-t", "--tabla", help="nombre tabla ods")
        parser.add_argument("-f", "--fecha", help="fecha de la particion en formato yyyymmdd, ejemplo 20181115, si no se indica se toma la fecha actual")
        return parser.parse_args()
if __name__ == '__main__':
        app_args = get_app_args()
        audit_alan_from_bq_to_hdfs(app_args)