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

# Parámetro global para limitar órdenes
TOTAL_ORDERS = 20  # Cambia este valor para ajustar el número total de órdenes
generated_orders = 0  # Contador de órdenes generadas

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

for usuario in usuarios:
    if generated_orders >= TOTAL_ORDERS:  # Detener si ya se alcanzó el límite global
        break

    tenant_id = usuario["tenant_id"]
    user_id = usuario["user_id"]
    user_info = generate_user_info()

    # Filtrar inventarios y productos que coincidan con el tenant_id del usuario
    inventarios_filtrados = [inv for inv in inventarios if inv["tenant_id"] == tenant_id]
    productos_filtrados = [prod for prod in productos if prod["tenant_id"] == tenant_id]

    if not inventarios_filtrados or not productos_filtrados:
        continue  # Saltar si no hay inventarios o productos para este tenant_id

    for _ in range(random.randint(1, 5)):  # Generar entre 1 y 5 órdenes por usuario
        if generated_orders >= TOTAL_ORDERS:  # Detener si ya se alcanzó el límite global
            break

        try:
            # Seleccionar un inventario y productos aleatorios
            inventario = random.choice(inventarios_filtrados)
            product_list = [
                {
                    "product_id": producto["product_id"],
                    "quantity": random.randint(1, 5),  # Cantidad aleatoria entre 1 y 5
                }
                for producto in random.sample(productos_filtrados, k=random.randint(1, 3))  # Seleccionar 1 a 3 productos
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
                Decimal(str(next(prod["product_price"] for prod in productos_filtrados if prod["product_id"] == product["product_id"])))
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

            # Agregar al archivo JSON
            orders.append(order)
            generated_orders += 1  # Incrementar el contador

        except ClientError as e:
            print(f"Error al insertar en la tabla pf_ordenes: {e.response['Error']['Message']}")

# Guardar en archivo JSON
with open(output_file_orders, "w", encoding="utf-8") as outfile:
    json.dump(orders, outfile, ensure_ascii=False, indent=4, default=str)

print(f"Órdenes generadas exitosamente. Guardadas en {output_file_orders} y subidas a DynamoDB.")
