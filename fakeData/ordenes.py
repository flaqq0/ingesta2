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
inventory_table = dynamodb.Table("pf_inventario")
products_table = dynamodb.Table("pf_productos")
users_table = dynamodb.Table("pf_usuarios")

# Salida
output_file_orders = "ordenes.json"

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
    start_date = datetime.now() - timedelta(days=365)  # Hace un año
    random_days = random.randint(0, 364)  # Excluye hoy
    return (start_date + timedelta(days=random_days)).isoformat()

# Obtener inventarios, productos y usuarios existentes
inventarios = get_all_items(inventory_table)
productos = get_all_items(products_table)
usuarios = get_all_items(users_table)

# Generar órdenes
generated_order_ids = set()
orders = []

for _ in range(10):  # Generar 200 órdenes
    try:
        # Seleccionar un inventario aleatorio
        inventario = random.choice(inventarios)
        tenant_id = inventario["tenant_id"]

        # Filtrar productos y usuarios que coincidan con el tenant_id
        productos_filtrados = [prod for prod in productos if prod["tenant_id"] == tenant_id]
        usuarios_filtrados = [user for user in usuarios if user["tenant_id"] == tenant_id]

        if not productos_filtrados or not usuarios_filtrados:
            continue  # Saltar si no hay productos o usuarios para este tenant_id

        producto = random.choice(productos_filtrados)
        usuario = random.choice(usuarios_filtrados)

        # Generar un order_id único
        while True:
            order_id = f"order_{random.randint(1000, 99999)}"
            if order_id not in generated_order_ids:
                generated_order_ids.add(order_id)
                break

        # Crear lista de productos
        product_list = [{
            "product_id": producto["product_id"],
            "quantity": random.randint(1, 5)  # Cantidad aleatoria entre 1 y 5
        }]

        # Generar datos para la orden
        user_info = generate_user_info()
        creation_date = generate_creation_date()
        shipping_date = (datetime.fromisoformat(creation_date) + timedelta(days=7)).isoformat()

        total_price = sum(
            Decimal(str(producto["product_price"])) * Decimal(str(product["quantity"]))
            for product in product_list
        )

        order = {
            "tenant_id": tenant_id,  # Tenant compartido entre producto, usuario y orden
            "order_id": order_id,
            "tu_id": f"{tenant_id}#{usuario['user_id']}",
            "user_id": usuario["user_id"],
            "user_info": user_info,
            "inventory_id": inventario["inventory_id"],
            "creation_date": creation_date,
            "shipping_date": shipping_date,
            "order_status": "PENDING",
            "products": product_list,
            "total_price": Decimal(str(total_price))
        }

        # Subir orden a DynamoDB
        orders_table.put_item(Item=order)

        # Agregar al archivo JSON
        orders.append(order)

    except ClientError as e:
        print(f"Error al insertar en la tabla pf_ordenes: {e.response['Error']['Message']}")

# Guardar en archivo JSON
with open(output_file_orders, "w", encoding="utf-8") as outfile:
    json.dump(orders, outfile, ensure_ascii=False, indent=4, default=str)

print(f"Datos generados exitosamente. Guardados en {output_file_orders} y subidos a DynamoDB.")
