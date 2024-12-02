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
table_ordenes = dynamodb.Table("pf_ordenes")

# Inicializar Faker
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

# Obtener productos en inventario
def get_existing_inventory():
    try:
        response = table_inventario.scan()
        return response.get("Items", [])
    except ClientError as e:
        print(f"Error al obtener inventarios: {e.response['Error']['Message']}")
        return []

# Generar información de usuario (departamento, provincia, distrito, dirección)
def generate_user_info():
    return {
        "departamento": fake.department(),
        "provincia": fake.state(),
        "distrito": fake.city(),
        "direccion": fake.street_address(),
    }

# Generar órdenes
def generate_orders(users, inventory):
    orders = []
    for _ in range(1):  # Generar 100 órdenes
        tenant_id = random.choice(tenants)

        # Seleccionar un usuario existente
        user = random.choice(users)
        user_id = user["user_id"]
        user_info = generate_user_info()

        # Seleccionar productos aleatorios del inventario
        inventory_items = random.sample(inventory, k=random.randint(1, 5))  # Seleccionar entre 1 y 5 productos
        product_list = []
        total_price = Decimal(0)

        for item in inventory_items:
            product_id = item["product_id"]
            inventory_id = item["inventory_id"]
            stock = item.get("stock", 10)  # Stock predeterminado si no está presente
            quantity = random.randint(1, min(stock, 5))  # Cantidad de producto menor o igual al stock

            # Actualizar el stock en DynamoDB
            new_stock = stock - quantity
            try:
                table_inventario.update_item(
                    Key={"tenant_id": tenant_id, "ip_id": f"{inventory_id}#{product_id}"},
                    UpdateExpression="SET stock = :new_stock",
                    ExpressionAttributeValues={":new_stock": new_stock},
                )
            except ClientError as e:
                print(f"Error al actualizar el stock de {product_id}: {e.response['Error']['Message']}")

            # Calcular precio total
            price = Decimal(str(item["product_price"]))
            total_price += price * quantity

            # Agregar producto a la lista
            product_list.append({"product_id": product_id, "quantity": quantity})

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
            "inventory_id": inventory_items[0]["inventory_id"],  # Tomar el inventario del primer producto
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
    # Obtener datos existentes
    users = get_existing_users()
    inventory = get_existing_inventory()

    if not users or not inventory:
        print("No se encontraron usuarios o productos existentes.")
        return

    # Generar órdenes
    orders = generate_orders(users, inventory)

    # Guardar en archivo JSON
    with open(output_file_orders, "w", encoding="utf-8") as outfile:
        json.dump(orders, outfile, ensure_ascii=False, indent=4, default=str)

    print(f"Archivo '{output_file_orders}' generado con éxito y las órdenes han sido subidas a DynamoDB.")

# Ejecutar el script
if __name__ == "__main__":
    main()
