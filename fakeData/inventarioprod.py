import json
import random
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

# Conexión a DynamoDB
region_name = "us-east-1"
dynamodb = boto3.resource("dynamodb", region_name=region_name)
table_inventarios = dynamodb.Table("pf_inventarios")
table_productos = dynamodb.Table("pf_productos")
table_inventario_productos = dynamodb.Table("pf_inventario")  # Tabla para la relación inventario-producto

# Lista de tenants
tenants = ["plazavea"]  # ["wong", "uwu"]

# Salida
output_file_inventory_products = "inventario_productos.json"

# Obtener todos los inventarios existentes
def get_existing_inventories():
    try:
        response = table_inventarios.scan()
        return response.get("Items", [])
    except ClientError as e:
        print(f"Error al obtener inventarios: {e.response['Error']['Message']}")
        return []

# Obtener todos los productos existentes
def get_existing_products():
    try:
        response = table_productos.scan()
        return response.get("Items", [])
    except ClientError as e:
        print(f"Error al obtener productos: {e.response['Error']['Message']}")
        return []

# Generar stock aleatorio basado en el stock disponible del inventario principal
def generate_stock(max_stock):
    if max_stock <= 1:
        return 1  # Si el stock máximo es <= 1, devuelve 1
    return random.randint(1, max_stock)

# Generar fake data para productos en inventarios
def generate_inventory_products(inventories, products, num_relations=10):
    inventory_products = []

    for inventory in inventories:
        tenant_id = inventory["tenant_id"]
        inventory_id = inventory["inventory_id"]
        inventory_stock = inventory.get("stock", 1000)  # Valor predeterminado si no existe stock

        # Filtrar productos por el mismo tenant_id
        tenant_products = [product for product in products if product["tenant_id"] == tenant_id]
        if not tenant_products:
            print(f"No se encontraron productos para el tenant {tenant_id}.")
            continue

        # Seleccionar un número definido de productos (num_relations)
        selected_products = []
        attempts = 0
        while len(selected_products) < num_relations and attempts < len(tenant_products) * 2:
            product = random.choice(tenant_products)
            if product not in selected_products:
                selected_products.append(product)
            attempts += 1

        for product in selected_products:
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
    inventory_products = generate_inventory_products(inventories, products, num_relations=10)

    # Guardar en inventario_productos.json
    with open(output_file_inventory_products, "w", encoding="utf-8") as outfile:
        json.dump(inventory_products, outfile, ensure_ascii=False, indent=4)

    print(f"Archivo '{output_file_inventory_products}' generado con éxito y los registros han sido subidos a DynamoDB.")

# Ejecutar el script
if __name__ == "__main__":
    main()
