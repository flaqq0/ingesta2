import boto3
import json
import random
from decimal import Decimal
from botocore.exceptions import ClientError

# Configurar DynamoDB
region_name = "us-east-1"
dynamodb = boto3.resource("dynamodb", region_name=region_name)

# Tablas DynamoDB
orders_table = dynamodb.Table("pf_ordenes")
payments_table = dynamodb.Table("pf_pagos")

# Salida
output_file_payments = "pagos.json"

# Parámetro global para limitar pagos
TOTAL_PAYMENTS = 20  # Número máximo de pagos
generated_payments = 0

# Obtener todas las órdenes de un usuario
def get_orders_by_user(user_id, tenant_id):
    try:
        response = orders_table.scan(
            FilterExpression="tenant_id = :tenant_id AND user_id = :user_id",
            ExpressionAttributeValues={
                ":tenant_id": tenant_id,
                ":user_id": user_id,
            },
        )
        return response.get("Items", [])
    except ClientError as e:
        print(f"Error al obtener órdenes para el usuario {user_id}: {e.response['Error']['Message']}")
        return []

# Generar pagos
generated_payment_ids = set()
payments = []

# Obtener todos los usuarios y sus órdenes
users_orders = {}

response = orders_table.scan()
orders = response.get("Items", [])

for order in orders:
    user_id = order["user_id"]
    tenant_id = order["tenant_id"]
    if (tenant_id, user_id) not in users_orders:
        users_orders[(tenant_id, user_id)] = []
    users_orders[(tenant_id, user_id)].append(order)

# Crear pagos para todas las órdenes de cada usuario
for (tenant_id, user_id), user_orders in users_orders.items():
    for order in user_orders:
        if generated_payments >= TOTAL_PAYMENTS:
            break
        if order.get("order_status") == "APPROVED PAYMENT":
            print(f"Saltando orden {order['order_id']}: Ya tiene el estado 'APPROVED PAYMENT'.")
            continue

        try:
            order_id = order["order_id"]
            user_info = order["user_info"]
            fecha_pago = order["creation_date"]  # Fecha de pago igual a creation_date de la orden
            total_price = Decimal(str(order["total_price"]))

            # Generar un pago_id único
            while True:
                pago_id = f"pago_{random.randint(10, 99999)}"
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
            generated_payments += 1

        except ClientError as e:
            print(f"Error al insertar en la tabla pf_pagos: {e.response['Error']['Message']}")

# Guardar en archivo JSON
with open(output_file_payments, "w", encoding="utf-8") as outfile:
    json.dump(payments, outfile, ensure_ascii=False, indent=4, default=str)

print(f"{generated_payments} pagos generados exitosamente. Guardados en {output_file_payments} y subidos a DynamoDB.")
