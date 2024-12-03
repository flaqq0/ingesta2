import boto3
import json
import os
from loguru import logger
from datetime import datetime

# Configuración de logger
LOG_FILE_PATH = "./logs/pull_inventory.log"
logger.add(LOG_FILE_PATH, format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {message}", level="INFO", rotation="10 MB")

# Variable global para definir el nombre de la tabla
TABLE_NAME = "pf_inventario"
REGION = "us-east-1"

def export_table_to_json_dynamodb(output_dir, table_name=TABLE_NAME):
    """
    Exporta los datos de una tabla DynamoDB a un archivo JSON.
    
    Args:
        output_dir (str): Directorio donde se guardará el archivo JSON.
        table_name (str): Nombre de la tabla DynamoDB.
    """
    logger.info(f"Iniciando exportación de la tabla '{table_name}' a JSON en el directorio '{output_dir}'.")
    start_time = datetime.now()

    try:
        # Crear cliente DynamoDB
        dynamodb = boto3.client('dynamodb', region_name=REGION)

        # Crear directorio de salida si no existe
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Directorio creado: {output_dir}")

        # Definir ruta del archivo JSON
        json_file_path = os.path.join(output_dir, f"{table_name}.json")

        all_items = []

        # Paginación para escanear todos los datos de la tabla
        paginator = dynamodb.get_paginator('scan')
        response_iterator = paginator.paginate(TableName=table_name)
        for page in response_iterator:
            items = page.get('Items', [])
            for item in items:
                # Convertir los elementos de DynamoDB a un formato JSON plano
                flat_item = {k: list(v.values())[0] for k, v in item.items()}
                all_items.append(flat_item)

        # Guardar los datos en un archivo JSON
        with open(json_file_path, 'w', encoding='utf-8') as json_file:
            json.dump(all_items, json_file, ensure_ascii=False, indent=4)

        logger.success(f"Exportación completada. Archivo generado: {json_file_path}. Total de registros exportados: {len(all_items)}")
    except Exception as e:
        logger.error(f"Error durante la exportación: {str(e)}")
    finally:
        end_time = datetime.now()
        logger.info(f"Exportación finalizada. Tiempo total: {end_time - start_time}")

# Llamar a la función para exportar los datos
output_dir = "./exported_data"
export_table_to_json_dynamodb(output_dir)
