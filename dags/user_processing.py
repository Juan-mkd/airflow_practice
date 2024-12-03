from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import requests
import zipfile
import os
import pandas as pd
import json

# Ruta donde se descargarán los archivos
DOWNLOAD_DIR = '/tmp/bybit_data'
ZIP_FILE = f'{DOWNLOAD_DIR}/trades_BTC_2024-02-12.zip'
JSONL_FILE = f'{DOWNLOAD_DIR}/trades_BTC_2024-02-12.jsonl'

def download_file(url):
    """Descarga el archivo ZIP desde la URL proporcionada."""
    try:
        response = requests.get(url)
        response.raise_for_status()  # Lanza un error si la solicitud falló
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        with open(ZIP_FILE, 'wb') as file:
            file.write(response.content)
        print(f"Archivo descargado: {ZIP_FILE}")
    except Exception as e:
        print(f"Error al descargar el archivo: {e}")
        raise

def extract_zip():
    """Descomprime el archivo ZIP descargado."""
    try:
        with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
            zip_ref.extractall(DOWNLOAD_DIR)
        print(f"Archivo extraído en: {DOWNLOAD_DIR}")
    except Exception as e:
        print(f"Error al extraer el archivo: {e}")
        raise

def list_files():
    """Lista los archivos en el directorio de descarga."""
    files = os.listdir(DOWNLOAD_DIR)
    print("Archivos en el directorio:")
    for file in files:
        print(file)

def read_jsonl_and_convert():
    """Lee el archivo JSONL y lo convierte en un DataFrame, luego muestra los encabezados y las primeras filas."""
    try:
        with open(JSONL_FILE, 'r') as file:
            data = [json.loads(line) for line in file]
        
        df = pd.json_normalize(data)
        
        # Mostrar los encabezados del DataFrame
        print("Encabezados del DataFrame:")
        print(df.columns.tolist())  # Esto mostrará los nombres de las columnas
        
        # Mostrar las primeras filas del DataFrame
        print("Primeras filas del DataFrame:")
        print(df.head())
    except Exception as e:
        print(f"Error al leer el archivo JSONL: {e}")
        raise

with DAG(
    dag_id="practicional_etl",
    start_date=datetime(2024, 12, 3),
    schedule_interval="@daily",
    catchup=False
) as dag:

    download_data = PythonOperator(
        task_id='download_data',
        python_callable=download_file,
        op_kwargs={'url': 'https://github.com/sferez/BybitMarketData/raw/main/data/BTC/2024-02-12/trades_BTC_2024-02-12.zip'}
    )

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_zip
    )

    list_data_files = PythonOperator(
        task_id='list_data_files',
        python_callable=list_files
    )

    read_jsonl = PythonOperator(
        task_id='read_jsonl',
        python_callable=read_jsonl_and_convert
    )

    download_data >> extract_data >> list_data_files >> read_jsonl
