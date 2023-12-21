import time
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import pandas as pd
import requests
from datetime import datetime
import boto3

#from sqlalchemy import create_engine, text
#import psycopg2
#from dotenv import load_dotenv
import os


def lambda_handler(event, context):

    BUCKET = os.environ['BUCKET']
    FILE =  os.environ['FILE']

    # Load dataframe    
    df, fecha = load_api_dataframe()
    df.to_csv(f'/tmp/{FILE}_{fecha}.csv', index = False)

    # Send file to  S3
    s3 = boto3.resource("s3")

    s3.Bucket(BUCKET).upload_file(f'/tmp/{FILE}_{fecha}.csv', Key=f'crypto/{FILE}_{fecha}.csv')
    os.remove(f'/tmp/{FILE}_{fecha}.csv')

    return f'{FILE}_{fecha}.csv send succeded'
    

def load_api_dataframe():
    # URL de la API
    url_crypto = 'https://api.coincap.io/v2/assets'
    response = requests.get(url_crypto).json()
    
    # Obteniendo la fecha
    fecha = datetime.fromtimestamp(response['timestamp'] / 1000).strftime("%Y-%m-%d")
    
    # Obteniendo en diccionario la lista de ranking de crytomonedas
    datos = []
    for item in response['data']:
        name = item['name']
        rank = int(item['rank'])
        priceUsd = round(float(item['priceUsd']), 2)
        datos.append((fecha, name, priceUsd, rank))    
    
    # Creando el dataframe
    col = ['fecha', 'nombre','precio','ranking']
    df = pd.DataFrame(datos,columns=col)
    df = df.sort_values(by = 'ranking',ascending = True)
    return df, fecha

    