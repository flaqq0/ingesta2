import boto3
import json
import random
from faker import Faker
from datetime import datetime, timedelta
from decimal import Decimal
from botocore.exceptions import ClientError

# Inicializar Faker
fake = Faker()

# Configurar DynamoDB
region_name = "us-east-1"
dynamodb = boto3.resource("dynamodb", region_name=region_name)

# Tablas DynamoDB
orders_table = dynamodb.Table("pf_ordenes")
reviews_table = dynamodb.Table("pf_comentario")

# Salida
output_file_reviews = "reviews.json"

# Parámetro global para limitar comentarios/reviews
TOTAL_REVIEWS = 10000  # Cambia este valor para ajustar el número total de comentarios
generated_reviews = 0

# Función para obtener todos los registros de una tabla DynamoDB
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
            table.delete_item(Key={"tenant_id": item["tenant_id"], "pr_id": item["pr_id"]})
        print(f"Todos los datos eliminados de la tabla {table.table_name}.")
    except ClientError as e:
        print(f"Error al eliminar datos de la tabla {table.table_name}: {e.response['Error']['Message']}")

# Eliminar datos previos de la tabla de comentarios
delete_all_items(reviews_table)

# Obtener órdenes existentes con estado "APPROVED PAYMENT"
orders = [
    order for order in get_all_items(orders_table)
    if order["order_status"] == "APPROVED PAYMENT"
]

# Generar comentarios
generated_review_ids = set()
reviews = []

for order in orders:
    if generated_reviews >= TOTAL_REVIEWS:
        break

    try:
        tenant_id = order["tenant_id"]
        user_id = order["user_id"]
        order_id = order["order_id"]
        inventory_id = order["inventory_id"]
        creation_date = datetime.fromisoformat(order["creation_date"])

        # Generar un comentario por producto en la orden
        for product in order["products"]:
            if generated_reviews >= TOTAL_REVIEWS:
                break

            product_id = product["product_id"]

            # Generar un review_id único
            while True:
                review_id = f"review_{random.randint(1000, 99999)}"
                if review_id not in generated_review_ids:
                    generated_review_ids.add(review_id)
                    break

            # Generar comentario y estrellas
            comentario = fake.sentence(nb_words=10)
            stars = random.randint(1, 5)
            last_modification = (creation_date + timedelta(days=1)).isoformat()

            # Crear el comentario/review
            review = {
                "tenant_id": tenant_id,
                "pr_id": f"{product_id}#${review_id}",
                "product_id": product_id,
                "review_id": review_id,
                "user_id": user_id,
                "comentario": comentario,
                "stars": Decimal(stars),
                "last_modification": last_modification,
            }

            # Subir comentario a DynamoDB
            reviews_table.put_item(Item=review)

            # Agregar al archivo JSON
            reviews.append(review)
            generated_reviews += 1

    except ClientError as e:
        print(f"Error al insertar en la tabla pf_comentario: {e.response['Error']['Message']}")

# Guardar en archivo JSON
with open(output_file_reviews, "w", encoding="utf-8") as outfile:
    json.dump(reviews, outfile, ensure_ascii=False, indent=4, default=str)

print(f"{generated_reviews} comentarios generados exitosamente. Guardados en {output_file_reviews} y subidos a DynamoDB.")
