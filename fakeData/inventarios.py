import json
import random
import csv
from faker import Faker
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal  # Importar Decimal para DynamoDB

# Inicializar Faker
fake = Faker()

# Lista de tenants
tenants = ["plazavea", "uwu", "wong"]

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
    return fake.sentence(nb_words=20)

# Leer datos del archivo Pere.csv
def load_peru_locations(file_path):
    locations = []
    with open(file_path, mode="r", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Saltar la cabecera
        for row in reader:
            departamento = row[0].strip()
            distrito = row[2].strip()
            locations.append(f"{departamento}-{distrito}")
    return locations

peru_locations = load_peru_locations("Pere.csv")
unique_peru_locations = list(set(peru_locations))  # Eliminar duplicados

# Generar inventarios
generated_inventory_ids = set()
inventories = []

# Inventarios para plazavea
for i in range(1): #inventarios a crear
    tenant_id = "plazavea"
    inventory_name = random.choice(unique_peru_locations)

    while True:
        inventory_id = f"inventory_{random.randint(1, 5500)}"
        if inventory_id not in generated_inventory_ids:
            generated_inventory_ids.add(inventory_id)
            break

    inventory = {
        "tenant_id": tenant_id,
        "inventory_id": inventory_id,
        "inventory_name": inventory_name,
        "stock": random_stock(),
        "observations": random_observations(),
    }
    inventories.append(inventory)

# Inventarios adicionales de plazavea (en otros países)
for i in range(1): # inventarios a crear
    tenant_id = "plazavea"
    inventory_name = f"{fake.state()}-{fake.city()}"

    while True:
        inventory_id = f"inventory_{random.randint(5501, 10600)}"
        if inventory_id not in generated_inventory_ids:
            generated_inventory_ids.add(inventory_id)
            break

    inventory = {
        "tenant_id": tenant_id,
        "inventory_id": inventory_id,
        "inventory_name": inventory_name,
        "stock": random_stock(),
        "observations": random_observations(),
    }
    inventories.append(inventory)

# Inventarios para wong
for i in range(1): #inventarios a crear
    tenant_id = "wong"
    inventory_name = unique_peru_locations[i]

    while True:
        inventory_id = f"inventory_{random.randint(10601, 16101)}"
        if inventory_id not in generated_inventory_ids:
            generated_inventory_ids.add(inventory_id)
            break

    inventory = {
        "tenant_id": tenant_id,
        "inventory_id": inventory_id,
        "inventory_name": inventory_name,
        "stock": random_stock(),
        "observations": random_observations(),
    }
    inventories.append(inventory)

# Inventarios adicionales de wong (en otros países)
for i in range(1): #inventarios a crear
    tenant_id = "wong"
    inventory_name = f"{fake.state()}-{fake.city()}"

    while True:
        inventory_id = f"inventory_{random.randint(16101, 21601)}"
        if inventory_id not in generated_inventory_ids:
            generated_inventory_ids.add(inventory_id)
            break

    inventory = {
        "tenant_id": tenant_id,
        "inventory_id": inventory_id,
        "inventory_name": inventory_name,
        "stock": random_stock(),
        "observations": random_observations(),
    }
    inventories.append(inventory)

# Inventarios para uwu (en provincias específicas)
provincias_lima = [loc for loc in unique_peru_locations if "Lima-" in loc]
provincias_arequipa = [loc for loc in unique_peru_locations if "Arequipa-" in loc]

for loc in provincias_lima + provincias_arequipa:
    tenant_id = "uwu"
    while True:
        inventory_id = f"inventory_{random.randint(21602, 27101)}"
        if inventory_id not in generated_inventory_ids:
            generated_inventory_ids.add(inventory_id)
            break

    inventory = {
        "tenant_id": tenant_id,
        "inventory_id": inventory_id,
        "inventory_name": loc,
        "stock": random_stock(),
        "observations": random_observations(),
    }
    inventories.append(inventory)

# Inventarios adicionales de uwu (en Estados Unidos, Reino Unido y España)
for i in range(1): #inventarios a crear
    tenant_id = "uwu"
    country = random.choice(["United States", "United Kingdom", "Spain"])
    inventory_name = f"{country}-{fake.city()}"

    while True:
        inventory_id = f"inventory_{random.randint(27102, 32601)}"
        if inventory_id not in generated_inventory_ids:
            generated_inventory_ids.add(inventory_id)
            break

    inventory = {
        "tenant_id": tenant_id,
        "inventory_id": inventory_id,
        "inventory_name": inventory_name,
        "stock": random_stock(),
        "observations": random_observations(),
    }
    inventories.append(inventory)

# Subir inventarios a DynamoDB
for inventory in inventories:
    try:
        table.put_item(Item=inventory)
    except ClientError as e:
        print(f"Error al agregar inventario {inventory['inventory_id']}: {e.response['Error']['Message']}")

# Guardar inventarios en archivo JSON
with open(output_file_inventories, "w", encoding="utf-8") as outfile:
    json.dump(inventories, outfile, ensure_ascii=False, indent=4)

print(f"Archivo '{output_file_inventories}' generado con éxito y los inventarios han sido subidos a DynamoDB.")
