import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from loguru import logger
from datetime import datetime

# Configuración del logger
LOG_FILE_PATH = "./logs/load_usuarios.log"
logger.add(
    LOG_FILE_PATH,
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {message}",
    level="INFO",
    rotation="10 MB"
)

# Configuración global
BUCKET_NAME = "aproyecto-dev"
BASE_DIRECTORY = "./exported_data"
FILE_NAME = "pf_usuarios.csv"
FILE_PATH = os.path.join(BASE_DIRECTORY, FILE_NAME)
S3_FILE_PATH = f"usuarios/{FILE_NAME}"

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
    logger.info(f"Iniciando carga al bucket '{BUCKET_NAME}'.")
    if not check_bucket_exists(BUCKET_NAME):
        logger.critical(f"El bucket '{BUCKET_NAME}' no está disponible. Abortando carga.")
        return

    start_time = datetime.now()

    if not os.path.exists(FILE_PATH):
        logger.error(f"El archivo '{FILE_PATH}' no existe. Abortando carga.")
        return
    
    logger.info(f"Archivo encontrado: '{FILE_PATH}'")
    try:
        upload_to_s3(FILE_PATH, BUCKET_NAME, S3_FILE_PATH)
    except Exception as e:
        logger.error(f"Error durante la carga del archivo '{FILE_PATH}': {str(e)}")
    finally:
        end_time = datetime.now()
        logger.success(f"Carga completada. Tiempo total: {end_time - start_time}")

# Llamar a la función principal
ingest()
