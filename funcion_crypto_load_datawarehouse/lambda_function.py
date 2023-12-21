import json
import pandas as pd
import psycopg2
import psycopg2.extras as extras 
import os
import urllib.parse
import boto3

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    
    # Get the object from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    print(key)

    # Se evalua el nombre del archivo esta en la carpeta 'crypto/'
    if 'crypto/' in key:

        # Get File from S3
        obj = s3_client.get_object(Bucket=bucket, Key= key)
        df = pd.read_csv(obj['Body']) # 'Body' is a key word
    
        fecha = key[-14:][:10]
        print(fecha)
    
        # Conectando a la BD    
        hostname= os.environ['HOSTNAME']
        database= os.environ['DATABASE']
        username= os.environ['USER']
        pwd= os.environ['CLAVE']
        port_id= os.environ['PORT_ID']
            
        conn = psycopg2.connect(host=hostname, dbname=database, user=username, password=pwd, port=port_id)
        
        # CARGA INCREMENTAL
    
        # Eliminando registros de esa "fecha"
        query=f""" DELETE FROM cryptomoneda WHERE fecha = '{fecha}'; """
        runExec(conn, query)
        
        # Añadiendo el dataframe a la tabla de la BD
        runExecMany(conn, df, 'cryptomoneda')    
        print('Exitoso')
        
    else:
        print('Nada')


def runQuery(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        columnas = [description[0] for description in cursor.description]
        result = cursor.fetchall()
        return pd.DataFrame(result, columns=columnas)
    except Exception as e:
        print(f"Error '{e}' ha ocurrido")
        
def runExec(connection, query):
    cursor = connection.cursor()
    try:
        result = cursor.execute(query)
        connection.commit()
    except Exception as e:
        print(f"Error '{e}' ha ocurrido")

def runExecMany(connection, df, table): 
    tuples = [tuple(x) for x in df.to_numpy()] 
    cols = ','.join(list(df.columns)) 
    # SQL query to execute 
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols) 
    cursor = connection.cursor() 
    try: 
        extras.execute_values(cursor, query, tuples) 
        connection.commit() 
    except (Exception, psycopg2.DatabaseError) as error: 
        print("Error: %s" % error) 
        connection.rollback() 
        cursor.close() 
        return 1
    print("the dataframe is inserted") 
    cursor.close() 