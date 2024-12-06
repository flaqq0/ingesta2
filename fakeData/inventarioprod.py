import boto3
import json
import random
from datetime import datetime
from botocore.exceptions import ClientError

# Configurar cliente de DynamoDB
region_name = "us-east-1"
dynamodb = boto3.resource("dynamodb", region_name=region_name)

# Tablas DynamoDB
inventarios_table = dynamodb.Table("pf_inventarios")
productos_table = dynamodb.Table("pf_productos")
inventario_producto_table = dynamodb.Table("pf_inventarioprod")  # Tabla donde se insertan los datos generados

# Salida
output_file = "productos_inventarios.json"

# Función para obtener todos los items de una tabla DynamoDB
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
            table.delete_item(Key={"tenant_id": item["tenant_id"], "ip_id": item["ip_id"]})
        print(f"Todos los datos eliminados de la tabla {table.table_name}.")
    except ClientError as e:
        print(f"Error al eliminar datos de la tabla {table.table_name}: {e.response['Error']['Message']}")

# Borrar datos existentes en la tabla pf_inventario
delete_all_items(inventario_producto_table)

# Obtener inventarios y productos existentes
inventarios = get_all_items(inventarios_table)
productos = get_all_items(productos_table)

# Agrupar inventarios y productos por tenant_id
tenant_inventarios = {}
tenant_productos = {}

for inv in inventarios:
    tenant_id = inv["tenant_id"]
    if tenant_id not in tenant_inventarios:
        tenant_inventarios[tenant_id] = []
    tenant_inventarios[tenant_id].append(inv)

for prod in productos:
    tenant_id = prod["tenant_id"]
    if tenant_id not in tenant_productos:
        tenant_productos[tenant_id] = []
    tenant_productos[tenant_id].append(prod)

# Generar datos para la tabla pf_inventario
productos_inventarios = []

for tenant_id in tenant_inventarios.keys():
    inventarios_list = tenant_inventarios[tenant_id]
    productos_list = tenant_productos.get(tenant_id, [])

    if not inventarios_list or not productos_list:
        print(f"Saltando tenant_id '{tenant_id}' porque no tiene inventarios o productos disponibles.")
        continue

    for inventario in inventarios_list:
        try:
            # Seleccionar 10 productos aleatorios del tenant_id
            selected_products = random.sample(productos_list, k=10)

            for producto in selected_products:
                # Generar stock aleatorio para el producto, respetando el stock del inventario
                max_stock = inventario.get("stock", 0)
                if max_stock == 0:
                    continue  # Saltar si el inventario no tiene stock disponible
                stock = random.randint(1, max_stock)

                # Crear registro para pf_inventario
                ip_id = f"{inventario['inventory_id']}#{producto['product_id']}"
                observaciones = f"Producto agregado al inventario {inventario['inventory_name']}."
                last_modification = datetime.now().isoformat()

                producto_inventario = {
                    "tenant_id": tenant_id,
                    "ip_id": ip_id,
                    "inventory_id": inventario["inventory_id"],
                    "product_id": producto["product_id"],
                    "stock": stock,
                    "last_modification": last_modification,
                    "observaciones": observaciones,
                }

                # Insertar en DynamoDB
                inventario_producto_table.put_item(Item=producto_inventario)

                # Agregar al archivo JSON
                productos_inventarios.append(producto_inventario)

        except ClientError as e:
            print(f"Error al insertar en la tabla pf_inventario: {e.response['Error']['Message']}")

# Guardar los datos generados en un archivo JSON
with open(output_file, "w", encoding="utf-8") as outfile:
    json.dump(productos_inventarios, outfile, ensure_ascii=False, indent=4)

print(f"Datos generados exitosamente. Guardados en {output_file} y subidos a DynamoDB.")
