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
            fecha = today.strftime("%Y-%m-%d")
            #print(fecha)
            partition = today.strftime("%Y%m%d")
            #print(partition)
            hora_servidor = today.strftime("%H")
        else:
            fecha= app_args.fecha
            partition = app_args.particion
            hora = app_args.hora

        #GET AUDIT FILE
        #fq = urllib.quote('filesystem_folder:('+fq_writers+') AND filesystem_folder:('+fq_hora_minutos+')')
        request = json.load(urllib2.urlopen("http://10.0.91.124:9003/api/count?operator=eq&sourceOrigen=bigquery&projectOrigen=dhh---global-service-alan&schemaOrigen="+app_args.dataset+"&tableNameOrigen="+app_args.tabla+"&dateFieldOrigen="+app_args.datefield+"&dateOrigen="+fecha+"&userDateFunctionOrigen=true&sourceDestino=datalake&schemaDestino=/alan/alan_hc_domicilios_prod&tableNameDestino=flow_session_events&cluster=hadoop-namenode-bi&fileCredentialsBQ=dhh---global-service-alan.json"))
        validacion = request['validacion']
        cantidad_registros_origen = request['cantRegistrosOrigen']
        cantidad_registros_destino = request['cantRegistrosDestino']
        if validacion:
            print('VALIDACION OK!')
            print('CANTIDAD REGISTROS BIGQUERY: ' + str(cantidad_registros_origen) + ' CANTIDAD REGISTROS HDFS: ' + str(cantidad_registros_destino))
        else:
            print('VALIDACION NOK!')
            print('CANTIDAD REGISTROS BIGQUERY: ' + str(cantidad_registros_origen) + ' CANTIDAD REGISTROS HDFS: ' + str(cantidad_registros_destino))
    except:
        print traceback.format_exc()

def get_app_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("-f", "--fecha", help="fecha de la particion en formato yyyy-mm-dd, ejemplo 20181115, si no se indica se toma la fecha actual")
        parser.add_argument("-p", "--particion", help="fecha de la particion en formato yyyymmdd, ejemplo 2018-11-15, si no se indica se toma la fecha actual")
        parser.add_argument("-o", "--hora", help="hora de la particion en hh, ejemplo 20,si no se indica se toma la hora actual")
        parser.add_argument("-d", "--dataset", help="nombre dataset bigquery")
        parser.add_argument("-t", "--tabla", help="nombre tabla bigquery")
        parser.add_argument("-c", "--datefield", help="nombre campo fecha para filtrar en bigquery")
        return parser.parse_args()
if __name__ == '__main__':
        app_args = get_app_args()
        audit_alan_from_bq_to_hdfs(app_args)