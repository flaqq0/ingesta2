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
inventory_table = dynamodb.Table("pf_inventarios")
inventario_producto_table = dynamodb.Table("pf_inventario")
users_table = dynamodb.Table("pf_usuarios")
products_table = dynamodb.Table("pf_productos")

# Salida
output_file_orders = "ordenes.json"

# Parámetro global para limitar órdenes
TOTAL_ORDERS = 12000
generated_orders = 0  # Contador de órdenes generadas

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
            table.delete_item(Key={"tenant_id": item["tenant_id"], "order_id": item["order_id"]})
        print(f"Todos los datos eliminados de la tabla {table.table_name}.")
    except ClientError as e:
        print(f"Error al eliminar datos de la tabla {table.table_name}: {e.response['Error']['Message']}")

# Eliminar datos previos de la tabla de órdenes
delete_all_items(orders_table)

'''

# Generar user_info
def generate_user_info():
    return {
        "pais": fake.country(),
        "ciudad": fake.city(),
        "direccion": fake.street_address(),
        "codigo_postal": fake.postcode(),
    }

# Generar fecha de creación aleatoria (no hoy)
def generate_creation_date():
    start_date = datetime.now() - timedelta(days=365)
    random_days = random.randint(0, 364)
    return (start_date + timedelta(days=random_days)).isoformat()

# Obtener inventarios, inventario-producto y usuarios existentes
inventarios = get_all_items(inventory_table)
inventario_producto = get_all_items(inventario_producto_table)
usuarios = get_all_items(users_table)

# Agrupar inventarios y productos por tenant_id
tenant_inventarios = {}
tenant_inventario_producto = {}
tenant_users = {}

for inv in inventarios:
    tenant_id = inv["tenant_id"]
    if tenant_id not in tenant_inventarios:
        tenant_inventarios[tenant_id] = []
    tenant_inventarios[tenant_id].append(inv)

for ip in inventario_producto:
    tenant_id = ip["tenant_id"]
    if tenant_id not in tenant_inventario_producto:
        tenant_inventario_producto[tenant_id] = []
    tenant_inventario_producto[tenant_id].append(ip)

for user in usuarios:
    tenant_id = user["tenant_id"]
    if tenant_id not in tenant_users:
        tenant_users[tenant_id] = []
    tenant_users[tenant_id].append(user)

# Generar órdenes
generated_order_ids = set()
orders = []

for tenant_id, user_list in tenant_users.items():
    if generated_orders >= TOTAL_ORDERS:
        break

    tenant_inv_list = tenant_inventarios.get(tenant_id, [])
    tenant_ip_list = tenant_inventario_producto.get(tenant_id, [])

    if not tenant_inv_list or not tenant_ip_list:
        print(f"Saltando tenant_id '{tenant_id}' porque no tiene inventarios o productos disponibles.")
        continue

    for user in user_list:
        if generated_orders >= TOTAL_ORDERS:
            break

        user_id = user["user_id"]
        user_info = generate_user_info()

        # Seleccionar un inventario aleatorio
        inventario = random.choice(tenant_inv_list)
        inventory_id = inventario["inventory_id"]

        # Filtrar productos que pertenecen al inventario seleccionado
        productos_filtrados = [
            ip for ip in tenant_ip_list if ip["inventory_id"] == inventory_id
        ]

        if not productos_filtrados:
            continue

        # Generar una orden
        try:
            product_list = []
            for producto in random.sample(productos_filtrados, k=min(3, len(productos_filtrados))):
                # Obtener precio desde la tabla de productos
                product_details = products_table.get_item(
                    Key={"tenant_id": tenant_id, "product_id": producto["product_id"]}
                )
                product_price = Decimal(product_details.get("Item", {}).get("product_price", 0))
                product_list.append(
                    {
                        "product_id": producto["product_id"],
                        "quantity": random.randint(1, 5),
                        "price": product_price,
                    }
                )

            total_price = sum(p["price"] * p["quantity"] for p in product_list)

            # Generar un order_id único
            while True:
                order_id = f"order_{random.randint(1000, 99999)}"
                if order_id not in generated_order_ids:
                    generated_order_ids.add(order_id)
                    break

            creation_date = generate_creation_date()
            shipping_date = (datetime.fromisoformat(creation_date) + timedelta(days=7)).isoformat()

            order = {
                "tenant_id": tenant_id,
                "order_id": order_id,
                "tu_id": f"{tenant_id}#{user_id}",
                "user_id": user_id,
                "user_info": user_info,
                "inventory_id": inventory_id,
                "creation_date": creation_date,
                "shipping_date": shipping_date,
                "order_status": "PENDING",
                "products": product_list,
                "total_price": Decimal(str(total_price)),
            }

            # Subir orden a DynamoDB
            orders_table.put_item(Item=order)

            # Agregar al archivo JSON
            orders.append(order)
            generated_orders += 1

        except ClientError as e:
            print(f"Error al insertar en la tabla pf_ordenes: {e.response['Error']['Message']}")

# Guardar en archivo JSON
with open(output_file_orders, "w", encoding="utf-8") as outfile:
    json.dump(orders, outfile, ensure_ascii=False, indent=4, default=str)

print(f"{generated_orders} órdenes generadas exitosamente. Guardadas en {output_file_orders} y subidas a DynamoDB.")
'''