import json
import random
from faker import Faker
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal  # Importar Decimal para DynamoDB

# Inicializar Faker
fake = Faker()

# Lista de tenants
tenants = ["uwu"] #, "wong", "plazavea"]

# Salida
output_file_inventories = "inventarios.json"

# Conexión a DynamoDB
region_name = "us-east-1"  # Cambia esta región según tu configuración
dynamodb = boto3.resource("dynamodb", region_name=region_name)
table = dynamodb.Table("pf_inventarios")  # Cambia por el nombre de tu tabla DynamoDB

# Función para generar un stock aleatorio
def random_stock():
    return random.randint(50, 10000)

# Función para generar observaciones aleatorias
def random_observations():
    return fake.sentence(nb_words=6)

# Generar inventarios
generated_inventory_ids = set()
inventories = []

for _ in range(4000):  # Generar 500 inventarios
    tenant_id = random.choice(tenants)

    # Generar un inventory_id único
    while True:
        inventory_id = f"inventory_{random.randint(12, 99999)}"
        if inventory_id not in generated_inventory_ids:
            generated_inventory_ids.add(inventory_id)
            break

    inventory_name = fake.company()
    stock = random_stock()
    observations = random_observations()

    inventory = {
        "tenant_id": tenant_id,
        "inventory_id": inventory_id,
        "inventory_name": inventory_name,
        "stock": stock,
        "observations": observations
    }
    inventories.append(inventory)

    # Subir cada inventario a DynamoDB
    try:
        table.put_item(Item=inventory)
    except ClientError as e:
        print(f"Error al agregar inventario {inventory_id}: {e.response['Error']['Message']}")

# Guardar en inventarios.json
with open(output_file_inventories, "w", encoding="utf-8") as outfile:
    json.dump(inventories, outfile, ensure_ascii=False, indent=4)

print(f"Archivo '{output_file_inventories}' generado con éxito y los inventarios han sido subidos a DynamoDB.")
