import json
import random
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

# Conexión a DynamoDB
region_name = "us-east-1"  # Cambia esta región según tu configuración
dynamodb = boto3.resource("dynamodb", region_name=region_name)
table_inventarios = dynamodb.Table("pf_inventarios")
table_productos = dynamodb.Table("pf_productos")
table_inventario_productos = dynamodb.Table("pf_inventario")  # Tabla para la relación inventario-producto

# Lista de tenants
tenants = ["plazavea"]  # ["wong", "uwu"]

# Salida
output_file_inventory_products = "inventario_productos.json"

# Obtener todos los `inventory_id` existentes
def get_existing_inventories():
    try:
        response = table_inventarios.scan()
        inventories = response.get("Items", [])
        return inventories
    except ClientError as e:
        print(f"Error al obtener inventarios: {e.response['Error']['Message']}")
        return []

# Obtener todos los `product_id` existentes
def get_existing_products():
    try:
        response = table_productos.scan()
        products = response.get("Items", [])
        return products
    except ClientError as e:
        print(f"Error al obtener productos: {e.response['Error']['Message']}")
        return []

# Generar stock aleatorio basado en el stock disponible del inventario principal
def generate_stock(max_stock):
    if max_stock <= 1:
        return 1  # Si el stock máximo es <= 1, devuelve 1
    return random.randint(1, max_stock)

# Generar fake data para productos en inventarios
def generate_inventory_products(inventories, products, target_count=10):
    inventory_products = []

    for _ in range(target_count):  # Intentar generar la cantidad deseada de relaciones
        while True:
            # Seleccionar un tenant aleatoriamente
            tenant_id = random.choice(tenants)

            # Filtrar inventarios por tenant_id
            tenant_inventories = [inventory for inventory in inventories if inventory["tenant_id"] == tenant_id]
            if not tenant_inventories:
                continue  # Si no hay inventarios para este tenant, repetir el bucle

            # Seleccionar un inventario existente
            inventory = random.choice(tenant_inventories)
            inventory_id = inventory["inventory_id"]
            inventory_stock = inventory.get("stock", 1000)  # Valor predeterminado si no existe stock

            # Filtrar productos por tenant_id
            tenant_products = [product for product in products if product["tenant_id"] == tenant_id]
            if not tenant_products:
                continue  # Si no hay productos para este tenant, repetir el bucle

            # Seleccionar un producto existente
            product = random.choice(tenant_products)
            product_id = product["product_id"]

            # Generar ip_id
            ip_id = f"{inventory_id}#{product_id}"

            # Generar stock para el producto
            stock = generate_stock(inventory_stock)

            # Observaciones aleatorias
            observaciones = f"Stock asignado para el inventario {inventory_id}"

            # Crear el registro
            inventory_product = {
                "tenant_id": tenant_id,
                "ip_id": ip_id,
                "inventory_id": inventory_id,
                "product_id": product_id,
                "stock": stock,
                "last_modification": datetime.now().isoformat(),
                "observaciones": observaciones,
            }

            inventory_products.append(inventory_product)

            # Subir a DynamoDB
            try:
                table_inventario_productos.put_item(Item=inventory_product)
            except ClientError as e:
                print(f"Error al agregar inventario-producto {ip_id}: {e.response['Error']['Message']}")

            # Salir del bucle interno al encontrar una relación válida
            break

    return inventory_products

# Función principal
def main():
    # Obtener inventarios y productos existentes
    inventories = get_existing_inventories()
    products = get_existing_products()

    if not inventories or not products:
        print("No se encontraron inventarios o productos existentes.")
        return

    # Generar fake data
    inventory_products = generate_inventory_products(inventories, products)

    # Guardar en inventario_productos.json
    with open(output_file_inventory_products, "w", encoding="utf-8") as outfile:
        json.dump(inventory_products, outfile, ensure_ascii=False, indent=4)

    print(f"Archivo '{output_file_inventory_products}' generado con éxito y los registros han sido subidos a DynamoDB.")

# Ejecutar el script
if __name__ == "__main__":
    main()
