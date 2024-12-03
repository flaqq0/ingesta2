import boto3
import csv
import os
from loguru import logger
from datetime import datetime

# Configuración de logger con milisegundos
LOG_FILE_PATH = "./logs/pull_orders.log"
logger.add(LOG_FILE_PATH, format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {message}", level="INFO", rotation="10 MB")

# Variable global para definir el nombre de la tabla
TABLE_NAME = "pf_ordenes"
REGION = "us-east-1"
BUCKET_NAME = "aproyecto-dev"

# Conexión a S3
s3 = boto3.client('s3')

# Función para subir archivo al bucket S3
def upload_to_s3(file_path, bucket, s3_file_path):
    try:
        s3.upload_file(file_path, bucket, s3_file_path)
        logger.info(f"Archivo '{file_path}' subido exitosamente a '{s3_file_path}' en el bucket '{bucket}'.")
    except Exception as e:
        logger.error(f"Error al subir el archivo '{file_path}' al bucket: {str(e)}")

def export_table_to_csv_dynamodb(output_dir, table_name=TABLE_NAME):
    """
    Exporta los datos de una tabla DynamoDB a un archivo CSV y lo sube a S3.
    
    Args:
        output_dir (str): Prefijo del directorio donde se guardará el archivo (dev, test, prod).
        table_name (str): Nombre de la tabla DynamoDB.
    """
    logger.info(f"Iniciando exportación de la tabla '{table_name}' a CSV para el prefijo '{output_dir}'.")
    start_time = datetime.now()

    try:
        # Crear cliente DynamoDB
        dynamodb = boto3.client('dynamodb', region_name=REGION)

        # Crear directorio de salida si no existe
        output_dir = f"./{output_dir}-{table_name}"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Directorio creado: {output_dir}")

        # Definir ruta del archivo CSV
        csv_file_path = os.path.join(output_dir, f"{table_name}.csv")

        # Obtener datos de la tabla
        all_items = []
        paginator = dynamodb.get_paginator('scan')
        response_iterator = paginator.paginate(TableName=table_name)

        for page in response_iterator:
            items = page.get('Items', [])
            all_items.extend(items)

        # Escribir datos en CSV
        if all_items:
            with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=all_items[0].keys())
                writer.writeheader()
                for item in all_items:
                    # Convertir valores de DynamoDB a un formato plano
                    flat_item = {k: list(v.values())[0] for k, v in item.items()}
                    writer.writerow(flat_item)

            logger.success(f"Exportación completada. Archivo generado: {csv_file_path}. Total de registros exportados: {len(all_items)}")

            # Subir a S3
            s3_file_path = f"ordenes/{table_name}.csv"
            upload_to_s3(csv_file_path, BUCKET_NAME, s3_file_path)

        else:
            logger.warning(f"No se encontraron datos en la tabla '{table_name}' para exportar.")

    except Exception as e:
        logger.error(f"Error durante la exportación: {str(e)}")
    finally:
        end_time = datetime.now()
        logger.info(f"Exportación finalizada. Tiempo total: {end_time - start_time}")

# Llamadas a la función con diferentes prefijos
output_dir = "./exported_data"  # Default "dev" if not set
export_table_to_csv_dynamodb(output_dir)
