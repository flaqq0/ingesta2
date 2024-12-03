import boto3
import json
import os
from loguru import logger
from datetime import datetime, timedelta

# Configuración de logger
LOG_FILE_PATH = "./logs/pull_inventory.log"
logger.add(
    LOG_FILE_PATH,
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {message}",
    level="INFO",
    rotation="10 MB"
)

# Variable global para definir el nombre de la tabla
TABLE_NAME = "pf_inventario"
REGION = "us-east-1"

def export_table_to_json_dynamodb(output_dir, table_name=TABLE_NAME):
    logger.info(f"Iniciando exportación de datos de la tabla '{table_name}' a JSON para el prefijo '{output_dir}'.")
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

        # Definir ruta del archivo JSON
        json_file_path = os.path.join(output_dir, f"{table_name}.json")

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

        # Guardar los datos en el archivo JSON
        with open(json_file_path, "w", encoding="utf-8") as json_file:
            json.dump(all_items, json_file, ensure_ascii=False, indent=4)

        logger.success(f"Exportación completada con éxito. Archivo guardado en {json_file_path}. Total de registros exportados: {len(all_items)}")
    except Exception as e:
        logger.error(f"Error durante la exportación: {str(e)}")
    finally:
        end_time = datetime.now()
        logger.info(f"Exportación finalizada. Tiempo total: {end_time - start_time}")

# Llamada a la función
export_table_to_json_dynamodb(output_dir="./exported_data")