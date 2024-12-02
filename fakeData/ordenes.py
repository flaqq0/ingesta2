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

# Obtener productos en inventario
def get_existing_inventory():
    try:
        response = table_inventario.scan()
        return response.get("Items", [])
    except ClientError as e:
        print(f"Error al obtener inventarios: {e.response['Error']['Message']}")
        return []

# Obtener el precio del producto desde la tabla `pf_productos`
def get_product_price(tenant_id, product_id):
    try:
        response = table_productos.get_item(Key={"tenant_id": tenant_id, "product_id": product_id})
        product = response.get("Item")
        if product and "product_price" in product:
            return Decimal(str(product["product_price"]))
        else:
            print(f"Advertencia: No se encontró precio para el producto {product_id}")
            return Decimal(0)  # Precio predeterminado si no está presente
    except ClientError as e:
        print(f"Error al obtener precio del producto {product_id}: {e.response['Error']['Message']}")
        return Decimal(0)

# Generar información de usuario (direcciones internacionales)
def generate_user_info():
    return {
        "pais": fake.country(),
        "ciudad": fake.city(),
        "direccion": fake.street_address(),
        "codigo_postal": fake.postcode(),
    }

# Incrementar el stock en el inventario
def update_inventory_stock(tenant_id, inventory_id, product_id, quantity):
    try:
        response = table_inventario.get_item(Key={"tenant_id": tenant_id, "ip_id": f"{inventory_id}#{product_id}"})
        if "Item" not in response:
            print(f"Advertencia: Producto {product_id} no encontrado en el inventario {inventory_id}")
            return

        current_stock = response["Item"].get("stock", 0)
        new_stock = current_stock + quantity

        table_inventario.update_item(
            Key={"tenant_id": tenant_id, "ip_id": f"{inventory_id}#{product_id}"},
            UpdateExpression="SET stock = :new_stock",
            ExpressionAttributeValues={":new_stock": new_stock},
        )
        print(f"Stock actualizado para {product_id} en inventario {inventory_id}: {new_stock}")
    except ClientError as e:
        print(f"Error al actualizar el stock de {product_id}: {e.response['Error']['Message']}")

# Generar órdenes
def generate_orders(users, inventory):
    orders = []
    for _ in range(100):  # Generar 100 órdenes
        tenant_id = random.choice(tenants)

        # Seleccionar un usuario existente
        user = random.choice(users)
        user_id = user["user_id"]
        user_info = generate_user_info()

        # Filtrar productos del mismo tenant_id
        tenant_inventory = [item for item in inventory if item["tenant_id"] == tenant_id]
        if not tenant_inventory:
            print(f"No hay productos disponibles para el tenant {tenant_id}")
            continue

        # Seleccionar productos aleatorios del inventario
        inventory_items = random.sample(tenant_inventory, k=random.randint(1, 5))  # Seleccionar entre 1 y 5 productos
        product_list = []
        total_price = Decimal(0)

        for item in inventory_items:
            product_id = item["product_id"]
            inventory_id = item["inventory_id"]
            stock = item.get("stock", 0)
            quantity = random.randint(1, 5)  # Cantidad aleatoria para ingresar al stock

            # Obtener el precio del producto desde `pf_productos`
            price = get_product_price(tenant_id, product_id)
            total_price += price * quantity

            # Actualizar el stock (incremento)
            update_inventory_stock(tenant_id, inventory_id, product_id, quantity)

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
