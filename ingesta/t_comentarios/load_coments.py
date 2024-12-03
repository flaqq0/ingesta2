import os
import json
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from loguru import logger
from datetime import datetime

# Configuración del logger
LOG_FILE_PATH = "./logs/load_comments.log"
logger.add(
    LOG_FILE_PATH,
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {message}",
    level="INFO",
    rotation="10 MB"
)
# Variables globales
BASE_DIRECTORY = "./exported_data"
BUCKET_NAME = "aproyecto-prod"

# Conexión a S3
s3 = boto3.client('s3')

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

    file_path = os.path.join(BASE_DIRECTORY, "pf_comentario.json")
    if not os.path.isfile(file_path):
        logger.warning(f"No se encontró el archivo 'pf_comentario.json' en '{BASE_DIRECTORY}'. Nada para subir.")
        return
    
    file_size = os.path.getsize(file_path) / 1024  # Tamaño en KB
    logger.info(f"Archivo '{file_path}' encontrado. Tamaño: {file_size:.2f} KB.")

    s3_file_path = "comentario/pf_comentario.json"
    try:
        logger.info(f"Subiendo archivo '{file_path}' al bucket S3 en la ruta '{s3_file_path}'.")
        upload_start_time = datetime.now()
        upload_to_s3(file_path, BUCKET_NAME, s3_file_path)
        upload_end_time = datetime.now()
        upload_duration = upload_end_time - upload_start_time
        logger.info(f"Archivo '{file_path}' subido exitosamente en {upload_duration.seconds} segundos.")
        processed_files += 1
    except Exception as e:
        logger.error(f"Error durante el procesamiento del archivo '{file_path}': {str(e)}")

    end_time = datetime.now()
    logger.success(f"Ingesta completada. Tiempo total: {end_time - start_time}. Archivos procesados: {processed_files}")

# Llamada a la función principal
ingest()