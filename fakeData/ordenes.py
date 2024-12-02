import json
import random
from datetime import datetime, timedelta
import boto3
from faker import Faker
from botocore.exceptions import ClientError
from decimal import Decimal

# Conexión a DynamoDB
region_name = "us-east-1"
dynamodb = boto3.resource("dynamodb", region_name=region_name)
table_usuarios = dynamodb.Table("pf_usuarios")
table_inventario = dynamodb.Table("pf_inventario")
table_productos = dynamodb.Table("pf_productos")
table_ordenes = dynamodb.Table("pf_ordenes")

# Inicializar Faker (direcciones internacionales)
fake = Faker()

# Lista de tenants
tenants = ["uwu", "plazavea", "wong"]

# Salida
output_file_orders = "ordenes.json"

# Obtener usuarios existentes
def get_existing_users():
    try:
        response = table_usuarios.scan()
        return response.get("Items", [])
    except ClientError as e:
        print(f"Error al obtener usuarios: {e.response['Error']['Message']}")
        return []

# Obtener productos cruzados con inventario
def get_products_with_inventory(tenant_id):
    try:
        # Obtener inventarios del tenant
        inventory_response = table_inventario.scan(
            FilterExpression="tenant_id = :tenant_id",
            ExpressionAttributeValues={":tenant_id": tenant_id}
        )
        inventory_items = inventory_response.get("Items", [])

        # Obtener productos del tenant
        products_response = table_productos.scan(
            FilterExpression="tenant_id = :tenant_id",
            ExpressionAttributeValues={":tenant_id": tenant_id}
        )
        product_items = products_response.get("Items", [])

        # Cruzar productos con inventarios
        products_with_inventory = []
        for inventory in inventory_items:
            for product in product_items:
                if product["product_id"] == inventory["product_id"]:
                    products_with_inventory.append({
                        "tenant_id": tenant_id,
                        "product_id": product["product_id"],
                        "inventory_id": inventory["inventory_id"],
                        "product_price": Decimal(str(product["product_price"])),
                        "product_name": product["product_name"]
                    })
        return products_with_inventory
    except ClientError as e:
        print(f"Error al cruzar productos e inventarios para tenant {tenant_id}: {e.response['Error']['Message']}")
        return []

# Generar información de usuario (direcciones internacionales)
def generate_user_info():
    return {
        "pais": fake.country(),
        "ciudad": fake.city(),
        "direccion": fake.street_address(),
        "codigo_postal": fake.postcode(),
    }

# Generar órdenes
def generate_orders(users):
    orders = []
    for _ in range(100):  # Generar 100 órdenes
        tenant_id = random.choice(tenants)

        # Seleccionar un usuario existente
        user = random.choice(users)
        user_id = user["user_id"]
        user_info = generate_user_info()

        # Obtener productos del tenant
        products_with_inventory = get_products_with_inventory(tenant_id)
        if not products_with_inventory:
            print(f"Advertencia: No se encontraron productos para el tenant {tenant_id}.")
            continue

        # Seleccionar productos aleatorios
        selected_products = random.sample(products_with_inventory, k=random.randint(1, 5))  # Entre 1 y 5 productos
        product_list = []
        total_price = Decimal(0)

        for product in selected_products:
            quantity = random.randint(1, 5)  # Cantidad aleatoria entre 1 y 5
            total_price += product["product_price"] * quantity

            product_list.append({
                "product_id": product["product_id"],
                "quantity": quantity,
                "product_name": product["product_name"]
            })

        # Generar IDs y fechas
        order_id = f"order_{random.randint(1000, 99999)}"
        creation_date = datetime.now()
        shipping_date = creation_date + timedelta(days=7)

        # Crear la orden
        order = {
            "tenant_id": tenant_id,
            "order_id": order_id,
            "user_id": user_id,
            "user_info": user_info,
            "products": product_list,
            "inventory_id": selected_products[0]["inventory_id"],  # Usar el inventario del primer producto
            "creation_date": creation_date.isoformat(),
            "shipping_date": shipping_date.isoformat(),
            "order_status": "PENDING",
            "total_price": total_price,
        }
        orders.append(order)

        # Subir a DynamoDB
        try:
            table_ordenes.put_item(Item=order)
        except ClientError as e:
            print(f"Error al subir la orden {order_id}: {e.response['Error']['Message']}")

    return orders

# Función principal
def main():
    # Obtener usuarios existentes
    users = get_existing_users()

    if not users:
        print("No se encontraron usuarios existentes.")
        return

    # Generar órdenes
    orders = generate_orders(users)

    # Guardar en archivo JSON
    with open(output_file_orders, "w", encoding="utf-8") as outfile:
        json.dump(orders, outfile, ensure_ascii=False, indent=4, default=str)

    print(f"Archivo '{output_file_orders}' generado con éxito y las órdenes han sido subidas a DynamoDB.")

# Ejecutar el script
if __name__ == "__main__":
    main()
