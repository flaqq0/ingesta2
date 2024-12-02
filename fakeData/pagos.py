import boto3
import json
import random
from datetime import datetime
from decimal import Decimal
from faker import Faker
from botocore.exceptions import ClientError

# Inicializar Faker
fake = Faker()

# Configurar DynamoDB
region_name = "us-east-1"
dynamodb = boto3.resource("dynamodb", region_name=region_name)

# Tablas DynamoDB
orders_table = dynamodb.Table("pf_ordenes")
payments_table = dynamodb.Table("pf_pagos")

# Salida
output_file_payments = "pagos.json"

# Obtener todos los registros de una tabla DynamoDB
def get_all_items(table):
    items = []
    try:
        response = table.scan()
        items.extend(response.get("Items", []))
        while "LastEvaluatedKey" in response:
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            items.extend(response.get("Items", []))
    except ClientError as e:
        print(f"Error al obtener datos de la tabla {table.table_name}: {e.response['Error']['Message']}")
    return items

# Obtener todas las órdenes existentes
orders = get_all_items(orders_table)

# Generar pagos
generated_payment_ids = set()
payments = []

for _ in range(20):  # Generar 200 pagos
    try:
        # Seleccionar una orden aleatoria
        order = random.choice(orders)

        tenant_id = order["tenant_id"]
        user_id = order["user_id"]
        order_id = order["order_id"]
        user_info = order["user_info"]
        fecha_pago = order["creation_date"]  # Fecha de pago igual a creation_date de la orden
        total_price = Decimal(str(order["total_price"]))

        # Generar un pago_id único
        while True:
            pago_id = f"pago_{random.randint(1000, 99999)}"
            if pago_id not in generated_payment_ids:
                generated_payment_ids.add(pago_id)
                break

        # Crear el pago
        payment = {
            "tenant_id": tenant_id,
            "pago_id": pago_id,
            "tu_id": f"{tenant_id}#{user_id}",
            "order_id": order_id,
            "user_id": user_id,
            "total": total_price,
            "fecha_pago": fecha_pago,
            "user_info": user_info,
        }

        # Subir pago a DynamoDB
        payments_table.put_item(Item=payment)

        # Actualizar el estado de la orden a 'APPROVED PAYMENT'
        orders_table.update_item(
            Key={"tenant_id": tenant_id, "order_id": order_id},
            UpdateExpression="SET order_status = :status",
            ExpressionAttributeValues={":status": "APPROVED PAYMENT"},
        )

        # Agregar al archivo JSON
        payments.append(payment)

    except ClientError as e:
        print(f"Error al insertar en la tabla pf_pagos: {e.response['Error']['Message']}")

# Guardar en archivo JSON
with open(output_file_payments, "w", encoding="utf-8") as outfile:
    json.dump(payments, outfile, ensure_ascii=False, indent=4, default=str)

print(f"Datos generados exitosamente. Guardados en {output_file_payments} y subidos a DynamoDB.")
