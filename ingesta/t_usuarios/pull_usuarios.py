import boto3
import csv
import os
from loguru import logger
from datetime import datetime

# Configuración de logger con milisegundos
LOG_FILE_PATH = "./logs/pull_users.log"
logger.add(
    LOG_FILE_PATH,
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {message}",
    level="INFO",
    rotation="10 MB"
)

# Variable global para definir el nombre de la tabla
TABLE_NAME = "pf_usuarios"
REGION = "us-east-1"

def export_table_to_csv_dynamodb(output_dir, table_name=TABLE_NAME):
    logger.info(f"Iniciando exportación de datos de la tabla '{table_name}' a csv para el prefijo '{output_dir}'.")
    start_time = datetime.now()

    try:
        dynamodb = boto3.client("dynamodb", region_name=REGION)

        # Crear directorio de salida si no existe
        output_dir = "./exported_data"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Directorio de salida creado: {output_dir}")
        else:
            logger.info(f"Directorio de salida ya existe: {output_dir}")

        # Definir ruta del archivo csv
        with open(OUTPUT_FILE, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = None
            paginator = dynamodb.get_paginator('scan')
            response_iterator = paginator.paginate(TableName=TABLE_NAME)

            for page in response_iterator:
                items = page.get('Items', [])
                
                # Crear encabezados a partir de las claves del primer elemento
                if writer is None and items:
                    headers = list(items[0].keys())
                    writer = csv.DictWriter(csvfile, fieldnames=headers)
                    writer.writeheader()

                # Escribir los datos en el archivo CSV
                for item in items:
                    flat_item = {k: list(v.values())[0] for k, v in item.items()}
                    writer.writerow(flat_item)
                    
        logger.success(f"Exportación completada con éxito. Archivo guardado en {csv_file_path}. Total de registros exportados: {len(all_items)}")
    except Exception as e:
        logger.error(f"Error durante la exportación: {str(e)}")
    finally:
        end_time = datetime.now()
        logger.info(f"Exportación finalizada. Tiempo total: {end_time - start_time}")

# Llamada a la función
export_table_to_csv_dynamodb(output_dir="./exported_data")