# script_proceso.py
import os
import pandas as pd
from sqlalchemy import create_engine
from azure.storage.blob import BlobServiceClient
import logging
from datetime import datetime

# CONFIGURACIÓN
SQL_USER = 'DB_USER'
SQL_PASSWORD = 'DB_PASS'
SQL_HOST = 'localhost'
SQL_PORT = '2638'
SQL_DB = 'database_name'

AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=YOUR_ACCOUNT;AccountKey=YOUR_KEY;EndpointSuffix=core.windows.net"
RAW_CONTAINER = "raw"
CLEANED_CONTAINER = "cleaned"

EXCEL_FOLDER = "./excels"
LOG_FILENAME = "log_proceso_datalake.log"
logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def log_info(msg): logging.info(msg); print(f"ℹ️ {msg}")
def log_error(msg): logging.error(msg); print(f"❌ {msg}")

def connect_blob_container(container_name):
    service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    return service.get_container_client(container_name)

def sqlanywhere_engine():
    return create_engine(f'sqlalchemy_sqlany://{SQL_USER}:{SQL_PASSWORD}@{SQL_HOST}:{SQL_PORT}/{SQL_DB}')

def upload_to_blob(local_path, blob_name, container):
    try:
        with open(local_path, "rb") as data:
            container.upload_blob(name=blob_name, data=data, overwrite=True)
        log_info(f"Subido correctamente: {blob_name}")
    except Exception as e:
        log_error(f"Error al subir {blob_name}: {e}")

def extract_sql_data(query, nombre_tabla):
    try:
        engine = sqlanywhere_engine()
        df = pd.read_sql(query, engine)
        raw_path = f"{nombre_tabla}_raw.parquet"
        clean_path = f"{nombre_tabla}_clean.parquet"
        df.to_parquet(raw_path, index=False)
        df_clean = df.dropna()
        df_clean.to_parquet(clean_path, index=False)
        return raw_path, clean_path
    except Exception as e:
        log_error(f"Error extrayendo datos SQL: {e}")
        return None, None

def extract_excel_files(folder):
    raw_paths = []
    clean_paths = []
    for file in os.listdir(folder):
        if file.endswith(".xlsx") or file.endswith(".xls"):
            try:
                file_path = os.path.join(folder, file)
                df = pd.read_excel(file_path)
                raw_file = file.replace('.xlsx', '_raw.parquet').replace('.xls', '_raw.parquet')
                clean_file = file.replace('.xlsx', '_clean.parquet').replace('.xls', '_clean.parquet')
                raw_path = os.path.join(folder, raw_file)
                clean_path = os.path.join(folder, clean_file)
                df.to_parquet(raw_path, index=False)
                df_clean = df.dropna()
                df_clean.to_parquet(clean_path, index=False)
                raw_paths.append(raw_path)
                clean_paths.append(clean_path)
                log_info(f"Procesado Excel: {file}")
            except Exception as e:
                log_error(f"Error al procesar Excel {file}: {e}")
    return raw_paths, clean_paths

def main():
    log_info("🟢 Inicio del proceso de integración al Data Lake")
    try:
        raw_container = connect_blob_container(RAW_CONTAINER)
        clean_container = connect_blob_container(CLEANED_CONTAINER)
        log_info("Contenedores conectados correctamente")

        # SQL
        log_info("Extrayendo datos de SQL Anywhere...")
        sql_raw, sql_clean = extract_sql_data("SELECT * FROM tu_tabla", "sql_table")
        if sql_raw and sql_clean:
            upload_to_blob(sql_raw, "sql/sql_table_raw.parquet", raw_container)
            upload_to_blob(sql_clean, "sql/sql_table_clean.parquet", clean_container)

        # Excel
        log_info("Procesando archivos Excel...")
        raw_excels, clean_excels = extract_excel_files(EXCEL_FOLDER)
        for path in raw_excels:
            upload_to_blob(path, f"excel/{os.path.basename(path)}", raw_container)
        for path in clean_excels:
            upload_to_blob(path, f"excel/{os.path.basename(path)}", clean_container)

        log_info("Proceso finalizado exitosamente")
    except Exception as e:
        log_error(f"Fallo general: {e}")
    log_info("🔴 Fin del proceso")

if __name__ == "__main__":
    main()
