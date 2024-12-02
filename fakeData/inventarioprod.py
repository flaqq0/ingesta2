import boto3
import random
import json
from datetime import datetime
from decimal import Decimal
from botocore.exceptions import ClientError

# Conexión a DynamoDB
region_name = "us-east-1"
dynamodb = boto3.resource("dynamodb", region_name=region_name)
table_inventarios = dynamodb.Table("pf_inventarios")
table_productos = dynamodb.Table("pf_productos")
table_inventario_prod = dynamodb.Table("pf_inventario")

# Salida
output_file = "inventarioprod.json"

# Obtener inventarios existentes
def get_existing_inventories():
    try:
        response = table_inventarios.scan()
        return response.get("Items", [])
    except ClientError as e:
        print(f"Error al obtener inventarios: {e.response['Error']['Message']}")
        return []

# Obtener productos existentes
def get_existing_products():
    try:
        response = table_productos.scan()
        return response.get("Items", [])
    except ClientError as e:
        print(f"Error al obtener productos: {e.response['Error']['Message']}")
        return []

# Generar relaciones entre inventarios y productos
def generate_inventory_products(inventories, products):
    inventory_products = []
    count = 0  # Contador para limitar a 10 datos
    for inventory in inventories:
        tenant_id = inventory["tenant_id"]
        inventory_id = inventory["inventory_id"]

        # Filtrar productos por el mismo tenant_id
        tenant_products = [product for product in products if product["tenant_id"] == tenant_id]
        if not tenant_products:
            continue  # Si no hay productos para este tenant, pasar al siguiente inventario

        # Seleccionar productos aleatorios
        selected_products = random.sample(tenant_products, k=random.randint(1, 5))  # Entre 1 y 5 productos

        for product in selected_products:
            product_id = product["product_id"]
            stock = random.randint(1, 500)  # Generar stock aleatorio

            # Crear relación inventario-producto
            inventory_product = {
                "tenant_id": tenant_id,
                "ip_id": f"{inventory_id}#{product_id}",
                "inventory_id": inventory_id,
                "product_id": product_id,
                "stock": stock,
                "last_modification": datetime.utcnow().isoformat(),
                "observaciones": "Stock asignado automáticamente",
            }
            inventory_products.append(inventory_product)

            # Subir a DynamoDB
            try:
                table_inventario_prod.put_item(Item=inventory_product)
                count += 1  # Incrementar contador
                if count >= 10:  # Detener después de 10 registros
                    return inventory_products
            except ClientError as e:
                print(f"Error al subir inventario-producto {inventory_product['ip_id']}: {e.response['Error']['Message']}")

    return inventory_products


# Función principal
def main():
    # Obtener datos existentes
    inventories = get_existing_inventories()
    products = get_existing_products()

    if not inventories or not products:
        print("No se encontraron inventarios o productos existentes.")
        return

    # Generar relaciones inventario-producto
    inventory_products = generate_inventory_products(inventories, products)

    # Guardar en archivo JSON
    with open(output_file, "w", encoding="utf-8") as outfile:
        json.dump(inventory_products, outfile, ensure_ascii=False, indent=4, default=str)

    print(f"Archivo '{output_file}' generado con éxito y las relaciones han sido subidas a DynamoDB.")

# Ejecutar el script
if __name__ == "__main__":
    main()
