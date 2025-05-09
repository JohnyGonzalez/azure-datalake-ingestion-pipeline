import os
import pandas as pd
from sqlalchemy import create_engine
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient
import logging
from datetime import datetime
import smtplib
from email.message import EmailMessage
import traceback

# CONFIGURACIÓN SQL
SQL_USER = 'DB_USER'
SQL_PASSWORD = 'DB_PASS'
SQL_HOST = 'localhost'
SQL_PORT = '2638'
SQL_DB = 'database_name'

# CONFIGURACIÓN BLOB
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=YOUR_ACCOUNT;AccountKey=YOUR_KEY;EndpointSuffix=core.windows.net"
RAW_CONTAINER = "raw"
CLEANED_CONTAINER = "cleaned"

# CONFIGURACIÓN DATALAKE
ADLS_CONNECTION_STRING = AZURE_CONNECTION_STRING  # Se puede usar la misma
ADLS_FILESYSTEM = "cleaned"  # Usa mismo contenedor si aplica
ADLS_CARPETA_METADATA = "metadata"

# CONFIGURACIÓN CORREO
CORREO_REMITENTE = "tucorreo@gmail.com"
CORREO_DESTINO = "destinatario@gmail.com"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "tucorreo@gmail.com"
SMTP_PASSWORD = "tu_contraseña_aplicacion"

# RUTAS LOCALES
EXCEL_FOLDER = "./excels"
RUTA_METADATA = "metadata"
LOG_FILENAME = "log_proceso_datalake.log"
logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def log_info(msg): logging.info(msg); print(f"ℹ️ {msg}")
def log_error(msg): logging.error(msg); print(f"❌ {msg}")

# --------------------
# FUNCIONES EXISTENTES
# --------------------

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

# -------------------------
# NUEVAS FUNCIONES AGREGADAS
# -------------------------

def registrar_metadata(ruta_directorio, nombre_csv):
    metadata = []

    for carpeta, _, archivos in os.walk(ruta_directorio):
        for archivo in archivos:
            ruta_completa = os.path.join(carpeta, archivo)
            estadisticas = os.stat(ruta_completa)
            metadata.append({
                "archivo": archivo,
                "carpeta": carpeta,
                "tamaño_bytes": estadisticas.st_size,
                "fecha_modificacion": datetime.fromtimestamp(estadisticas.st_mtime)
            })

    df = pd.DataFrame(metadata)
    os.makedirs(RUTA_METADATA, exist_ok=True)
    ruta_salida = os.path.join(RUTA_METADATA, nombre_csv)
    df.to_csv(ruta_salida, index=False)
    log_info(f"Metadata registrada: {ruta_salida}")
    return ruta_salida

def subir_metadata_a_datalake(ruta_local, nombre_destino):
    try:
        service_client = DataLakeServiceClient.from_connection_string(ADLS_CONNECTION_STRING)
        file_system_client = service_client.get_file_system_client(file_system=ADLS_FILESYSTEM)
        directorio = file_system_client.get_directory_client(ADLS_CARPETA_METADATA)
        archivo = directorio.create_file(nombre_destino)

        with open(ruta_local, 'rb') as file_data:
            archivo.append_data(file_data.read(), offset=0, length=os.path.getsize(ruta_local))
            archivo.flush_data(os.path.getsize(ruta_local))
        log_info(f"Metadata subida al Data Lake como '{nombre_destino}'")
    except Exception as e:
        log_error(f"Error subiendo metadata al Data Lake: {e}")
        raise

def enviar_correo_error(asunto, mensaje):
    msg = EmailMessage()
    msg.set_content(mensaje)
    msg['Subject'] = asunto
    msg['From'] = CORREO_REMITENTE
    msg['To'] = CORREO_DESTINO
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
        log_info("Correo de error enviado.")
    except Exception as e:
        log_error(f"No se pudo enviar el correo: {e}")

# --------------------
# MAIN
# --------------------

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

        # Metadata
        log_info("Registrando metadata de archivos en 'cleaned'...")
        nombre_metadata = f"metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        ruta_metadata = registrar_metadata(EXCEL_FOLDER, nombre_metadata)
        subir_metadata_a_datalake(ruta_metadata, nombre_metadata)

        log_info("✅ Proceso finalizado exitosamente")

    except Exception as e:
        error_msg = traceback.format_exc()
        log_error(f"Fallo general del proceso: {e}")
        enviar_correo_error(
            asunto="❌ Error en ETL hacia Data Lake",
            mensaje=f"Ocurrió un error:\n\n{error_msg}"
        )

    log_info("🔴 Fin del proceso")

if __name__ == "__main__":
    main()
