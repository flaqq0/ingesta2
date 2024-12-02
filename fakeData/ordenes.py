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

output_file_orders = "ordenes.json"

# Parámetro global para limitar órdenes
TOTAL_ORDERS = 20

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

# Validación de datos
if not usuarios or not inventarios or not productos:
    print("No hay datos suficientes en DynamoDB para generar órdenes.")
    exit(1)

# Generar exactamente 20 órdenes
generated_orders = []
generated_order_ids = set()

for _ in range(TOTAL_ORDERS):
    # Seleccionar usuario aleatorio
    usuario = random.choice(usuarios)
    tenant_id = usuario["tenant_id"]
    user_id = usuario["user_id"]
    user_info = generate_user_info()

    # Filtrar inventarios y productos que coincidan con el tenant_id del usuario
    inventarios_filtrados = [inv for inv in inventarios if inv["tenant_id"] == tenant_id]
    if not inventarios_filtrados:
        print(f"Saltando usuario {user_id}: No hay inventarios válidos para tenant_id {tenant_id}")
        continue

    productos_disponibles = [
        prod for prod in productos
        if any(inv["product_id"] == prod["product_id"] for inv in inventarios_filtrados)
    ]
    if not productos_disponibles:
        print(f"Saltando usuario {user_id}: No hay productos disponibles en el inventario para tenant_id {tenant_id}")
        continue

    try:
        # Seleccionar un inventario y productos aleatorios
        inventario = random.choice(inventarios_filtrados)
        product_list = [
            {
                "product_id": producto["product_id"],
                "quantity": random.randint(1, 5),  # Cantidad aleatoria entre 1 y 5
            }
            for producto in random.sample(productos_disponibles, k=random.randint(1, 3))  # Seleccionar 1 a 3 productos
        ]

        # Generar un order_id único
        while True:
            order_id = f"order_{random.randint(1000, 99999)}"
            if order_id not in generated_order_ids:
                generated_order_ids.add(order_id)
                break

        # Generar datos para la orden
        creation_date = generate_creation_date()
        shipping_date = (datetime.fromisoformat(creation_date) + timedelta(days=7)).isoformat()

        total_price = sum(
            Decimal(str(next(prod["product_price"] for prod in productos_disponibles if prod["product_id"] == product["product_id"])))
            * Decimal(product["quantity"])
            for product in product_list
        )

        order = {
            "tenant_id": tenant_id,
            "order_id": order_id,
            "tu_id": f"{tenant_id}#{user_id}",
            "user_id": user_id,
            "user_info": user_info,
            "inventory_id": inventario["inventory_id"],
            "creation_date": creation_date,
            "shipping_date": shipping_date,
            "order_status": "PENDING",
            "products": product_list,
            "total_price": Decimal(str(total_price)),
        }

        # Subir orden a DynamoDB
        orders_table.put_item(Item=order)
        generated_orders.append(order)

    except ClientError as e:
        print(f"Error al insertar en la tabla pf_ordenes: {e.response['Error']['Message']}")

# Guardar en archivo JSON
with open(output_file_orders, "w", encoding="utf-8") as outfile:
    json.dump(generated_orders, outfile, ensure_ascii=False, indent=4, default=float)

print(f"{len(generated_orders)} órdenes generadas exitosamente. Guardadas en {output_file_orders} y subidas a DynamoDB.")
