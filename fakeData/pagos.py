import boto3
import json
import random
from faker import Faker
from decimal import Decimal
from datetime import datetime, timedelta
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

# Parámetro global para limitar pagos
TOTAL_PAYMENTS = 20  # Número máximo de pagos
generated_payments = 0

# Función para obtener todos los registros de una tabla DynamoDB
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

# Función para eliminar todos los datos de una tabla DynamoDB
def delete_all_items(table):
    try:
        items = get_all_items(table)
        for item in items:
            table.delete_item(Key={"tenant_id": item["tenant_id"], "pago_id": item["pago_id"]})
        print(f"Todos los datos eliminados de la tabla {table.table_name}.")
    except ClientError as e:
        print(f"Error al eliminar datos de la tabla {table.table_name}: {e.response['Error']['Message']}")

# Eliminar datos previos de la tabla de pagos
delete_all_items(payments_table)

def generate_payment_method():
    payment_methods = ["TARJETA", "YAPE", "MERCADO PAGO", "PAYPAL", "PLIN", "TRANSFERENCIA BANCARIA", "GOOGLE PAY", "APPLE PAY", "BITCOIN", "QR"]
    selected_method = random.choice(payment_methods)
    
    if selected_method == "TARJETA":
        return {
            "metodo": "TARJETA",
            "card_number": fake.credit_card_number(),
            "pin": random.randint(100, 999),
            "billing_address": fake.address(),
        }
    elif selected_method == "YAPE":
        return {
            "metodo": "YAPE",
            "telf_number": fake.phone_number(),
            "code": random.randint(100000, 999999),
        }
    elif selected_method == "MERCADO PAGO":
        return {
            "metodo": "MERCADO PAGO",
            "account_email": fake.email(),
            "transaction_token": "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890", k=12)),
            "payment_reference": f"REF{random.randint(100000, 999999)}",
        }
    elif selected_method == "PAYPAL":
        return {
            "metodo": "PAYPAL",
            "paypal_email": fake.email(),
            "transaction_id": f"TXN{random.randint(100000, 999999)}",
        }
    elif selected_method == "PLIN":
        return {
            "metodo": "PLIN",
            "telf_number": fake.phone_number(),
            "transaction_code": f"PLIN{random.randint(100000, 999999)}",
        }
    elif selected_method == "TRANSFERENCIA BANCARIA":
        return {
            "metodo": "TRANSFERENCIA BANCARIA",
            "bank_name": fake.company(),
            "account_number": fake.bban(),
            "transaction_id": f"TRX{random.randint(100000, 999999)}",
            "swift_code": "SWIFT123",
        }
    elif selected_method == "GOOGLE PAY":
        return {
            "metodo": "GOOGLE PAY",
            "google_pay_id": fake.uuid4(),
            "transaction_token": "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890", k=12)),
        }
    elif selected_method == "APPLE PAY":
        return {
            "metodo": "APPLE PAY",
            "apple_pay_id": fake.uuid4(),
            "device_token": "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890", k=12)),
        }
    elif selected_method == "BITCOIN":
        return {
            "metodo": "BITCOIN",
            "wallet_address": fake.uuid4(),
            "transaction_hash": "".join(random.choices("0123456789abcdef", k=64)),
            "amount_btc": round(random.uniform(0.001, 0.01), 8),
        }
    elif selected_method == "QR":
        return {
            "metodo": "QR",
            "qr_code_reference": f"QR{random.randint(100000, 999999)}",
            "transaction_id": f"TXN{random.randint(100000, 999999)}",
        }


# Obtener órdenes existentes
orders = get_all_items(orders_table)

# Agrupar órdenes por tenant_id y user_id
users_orders = {}
for order in orders:
    user_id = order["user_id"]
    tenant_id = order["tenant_id"]
    if (tenant_id, user_id) not in users_orders:
        users_orders[(tenant_id, user_id)] = []
    users_orders[(tenant_id, user_id)].append(order)

# Crear pagos para todas las órdenes
generated_payment_ids = set()
payments = []

for (tenant_id, user_id), user_orders in users_orders.items():
    for order in user_orders:
        if generated_payments >= TOTAL_PAYMENTS:
            break

        # Verificar si la orden ya tiene el estado "APPROVED PAYMENT"
        if order.get("order_status") == "APPROVED PAYMENT":
            print(f"Saltando orden {order['order_id']}: Ya tiene el estado 'APPROVED PAYMENT'.")
            continue

        try:
            order_id = order["order_id"]
            total_price = Decimal(str(order["total_price"]))
            creation_date = datetime.fromisoformat(order["creation_date"])

            # Generar fecha de pago (máximo 2 días después de la creación)
            fecha_pago = creation_date + timedelta(days=random.randint(0, 2))
            fecha_pago_iso = fecha_pago.isoformat()

            # Generar un pago_id único
            while True:
                pago_id = f"pago_{random.randint(10, 99999)}"
                if pago_id not in generated_payment_ids:
                    generated_payment_ids.add(pago_id)
                    break

            # Crear datos de pago
            payment = {
                "tenant_id": tenant_id,
                "pago_id": pago_id,
                "order_id": order_id,
                "user_id": user_id,
                "user_info": generate_payment_method(),
                "total": total_price,
                "fecha_pago": fecha_pago_iso,
            }

            # Subir pago a DynamoDB
            payments_table.put_item(Item=payment)

            # Actualizar el estado de la orden a "APPROVED PAYMENT"
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
