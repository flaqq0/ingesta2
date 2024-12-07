import boto3
import csv
import os
from loguru import logger
from datetime import datetime

# Configuración de logger con milisegundos
LOG_FILE_PATH = "./logs/pull_comments.log"
logger.add(
    LOG_FILE_PATH,
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {message}",
    level="INFO",
    rotation="10 MB"
)

# Variable global para definir el nombre de la tabla
TABLE_NAME = "pf_comentario"
REGION = "us-east-1"

def export_table_to_csv_dynamodb(output_dir, table_name=TABLE_NAME):
    logger.info(f"Iniciando exportación de datos de la tabla '{table_name}' a CSV para el prefijo '{output_dir}'.")
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

        # Definir ruta del archivo CSV
        csv_file_path = os.path.join(output_dir, f"{table_name}.csv")

        all_items = []
        paginator = dynamodb.get_paginator("scan")
        response_iterator = paginator.paginate(TableName=table_name)

        logger.info("Comenzando escaneo de la tabla DynamoDB...")
        for page_number, page in enumerate(response_iterator, start=1):
            logger.info(f"Procesando página {page_number}...")
            items = page.get("Items", [])
            logger.info(f"Página {page_number} contiene {len(items)} elementos.")
            for item_number, item in enumerate(items, start=1):
                try:
                    flat_item = {k: list(v.values())[0] for k, v in item.items()}
                    all_items.append(flat_item)
                except Exception as item_error:
                    logger.warning(f"Error procesando elemento {item_number} en página {page_number}: {str(item_error)}")

        # Escribir los datos en formato CSV con delimitador ";"
        if all_items:
            with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=all_items[0].keys(), delimiter=';')
                writer.writeheader()
                writer.writerows(all_items)
            logger.success(f"Exportación completada con éxito. Archivo guardado en {csv_file_path}. Total de registros exportados: {len(all_items)}")
        else:
            logger.warning("No se encontraron registros para exportar.")
    except Exception as e:
        logger.error(f"Error durante la exportación: {str(e)}")
    finally:
        end_time = datetime.now()
        logger.info(f"Exportación finalizada. Tiempo total: {end_time - start_time}")

# Llamada a la función
export_table_to_csv_dynamodb(output_dir="./exported_data")
