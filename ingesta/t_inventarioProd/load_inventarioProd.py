import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from loguru import logger
from datetime import datetime

# Configuración de logger
LOG_FILE_PATH = "./logs/load_inventoryProd.log"
logger.add(LOG_FILE_PATH, format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {message}", level="INFO", rotation="10 MB")

# Variables globales
BASE_DIRECTORY = "./exported_data"  # Directorio base donde se encuentran los archivos locales
BUCKET_NAME = "aproyecto-dev"  # Bucket S3 donde se subirán los datos

# Conexión a S3
s3 = boto3.client('s3')

# Función para verificar la existencia del bucket
def check_bucket_exists(bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        logger.info(f"El bucket '{bucket_name}' existe y está accesible.")
        return True
    except ClientError as e:
        logger.error(f"Error al acceder al bucket '{bucket_name}': {str(e)}")
        return False

# Función para subir un archivo a S3
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

# Función principal para realizar la ingesta
def ingest():
    logger.info(f"Iniciando ingesta al bucket '{BUCKET_NAME}'.")
    
    # Verificar si el bucket existe
    if not check_bucket_exists(BUCKET_NAME):
        logger.critical(f"El bucket '{BUCKET_NAME}' no está disponible. Abortando ingesta.")
        return

    start_time = datetime.now()
    processed_files = 0

    # Verificar si el directorio BASE_DIRECTORY existe
    if not os.path.exists(BASE_DIRECTORY):
        logger.error(f"El directorio '{BASE_DIRECTORY}' no existe. Abortando ingesta.")
        return

    # Buscar el archivo JSON en el directorio
    file_path = os.path.join(BASE_DIRECTORY, "pf_inventario.json")
    if not os.path.isfile(file_path):
        logger.warning(f"No se encontró el archivo 'pf_inventario.json' en '{BASE_DIRECTORY}'. Nada para subir.")
        return

    # Subir archivo al bucket S3
    s3_file_path = "inventarioProd/pf_inventario.json"  # Ruta en el bucket S3
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
