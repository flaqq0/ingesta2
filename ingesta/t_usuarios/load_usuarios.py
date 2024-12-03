import os
import json
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from loguru import logger
from datetime import datetime

# Configuración del logger
LOG_FILE_PATH = "./logs/load_usuarios.log"
logger.add(LOG_FILE_PATH, format="{time:2024-11-30 HH:mm:ss.SSS} | {level} | {message}", level="INFO", rotation="10 MB")

# Variables globales
BASE_DIRECTORY = "./exported_data"
BUCKET_NAME = "aproyecto-dev"

# Conexión a S3
s3 = boto3.client("s3")

def check_bucket_exists(bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        logger.info(f"El bucket '{bucket_name}' existe y está accesible.")
        return True
    except ClientError as e:
        logger.error(f"Error al acceder al bucket '{bucket_name}': {str(e)}")
        return False

def upload_to_s3(file_path, bucket, s3_file_path):
    try:
        s3.upload_file(file_path, bucket, s3_file_path)
        logger.info(f"Archivo '{file_path}' subido exitosamente a '{s3_file_path}' en el bucket '{bucket}'.")
    except FileNotFoundError:
        logger.error(f"El archivo '{file_path}' no fue encontrado.")
    except NoCredentialsError:
        logger.critical("Credenciales de AWS no disponibles.")
    except ClientError as e:
        logger.error(f"Error de cliente al subir el archivo '{file_path}': {str(e)}")
    except Exception as e:
        logger.error(f"Error desconocido al subir el archivo '{file_path}': {str(e)}")

def ingest():
    logger.info(f"Iniciando ingesta al bucket '{BUCKET_NAME}'.")
    if not check_bucket_exists(BUCKET_NAME):
        logger.critical(f"El bucket '{BUCKET_NAME}' no está disponible. Abortando ingesta.")
        return

    start_time = datetime.now()
    processed_files = 0

    if not os.path.exists(BASE_DIRECTORY):
        logger.error(f"El directorio '{BASE_DIRECTORY}' no existe. Abortando ingesta.")
        return

    file_path = os.path.join(BASE_DIRECTORY, "usuarios.json")
    if not os.path.isfile(file_path):
        logger.warning(f"No se encontró el archivo 'usuarios.json' en '{BASE_DIRECTORY}'. Nada para subir.")
        return

    s3_file_path = "usuarios/usuarios.json"
    try:
        logger.info(f"Procesando archivo: {file_path}")
        upload_to_s3(file_path, BUCKET_NAME, s3_file_path)
        processed_files += 1
    except Exception as e:
        logger.error(f"Error al procesar el archivo '{file_path}': {str(e)}")

    end_time = datetime.now()
    logger.success(f"Ingesta completada. Tiempo total: {end_time - start_time}. Archivos procesados: {processed_files}")

# Llamada a la función principal
ingest()
