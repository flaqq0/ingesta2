import boto3
import csv
import os
from loguru import logger
from datetime import datetime

# Configuración de logger
LOG_FILE_PATH = "./logs/pull_users.log"
logger.add(
    LOG_FILE_PATH,
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {message}",
    level="INFO",
    rotation="10 MB"
)

# Configuración global
TABLE_NAME = "pf_usuarios"
REGION = "us-east-1"
OUTPUT_DIR = "./exported_data"
OUTPUT_FILE = f"{OUTPUT_DIR}/{TABLE_NAME}.csv"

def export_table_to_csv_dynamodb():
    logger.info(f"Iniciando exportación de datos de la tabla '{TABLE_NAME}' a CSV.")
    start_time = datetime.now()

    try:
        dynamodb = boto3.client("dynamodb", region_name=REGION)

        # Crear directorio de salida si no existe
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
            logger.info(f"Directorio creado: {OUTPUT_DIR}")
        else:
            logger.info(f"Directorio ya existe: {OUTPUT_DIR}")

        # Definir archivo CSV
        with open(OUTPUT_FILE, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = None
            paginator = dynamodb.get_paginator('scan')
            response_iterator = paginator.paginate(TableName=TABLE_NAME)

            for page in response_iterator:
                items = page.get('Items', [])
                if writer is None and items:
                    headers = list(items[0].keys())
                    writer = csv.DictWriter(csvfile, fieldnames=headers)
                    writer.writeheader()

                for item in items:
                    flat_item = {k: list(v.values())[0] for k, v in item.items()}
                    writer.writerow(flat_item)

        logger.success(f"Exportación completada. Archivo guardado en '{OUTPUT_FILE}'.")
    except Exception as e:
        logger.error(f"Error durante la exportación: {str(e)}")
    finally:
        end_time = datetime.now()
        logger.info(f"Exportación finalizada. Tiempo total: {end_time - start_time}")

# Llamar a la función
export_table_to_csv_dynamodb()
