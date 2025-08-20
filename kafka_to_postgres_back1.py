import os
import json
import time
import psycopg2
from kafka import KafkaConsumer
import re  

# Configuración de variables de entorno
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Configuración de la base de datos
DB_TABLE = "data_from_kafka"

# Conexión a Kafka
try:
    consumer = KafkaConsumer(
#        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-postgres-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    consumer.subscribe(pattern=re.compile(f"{KAFKA_TOPIC}.*"))
    print("Conexión a Kafka exitosa.")

except Exception as e:
    print(f"Error al conectar a Kafka: {e}")
    exit()

# Conexión a PostgreSQL
conn = None
cursor = None
try:
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = conn.cursor()
    print("Conexión a PostgreSQL exitosa.")

    # Bucle infinito para escuchar y procesar mensajes
    while True:
        # Busca mensajes por un tiempo limitado (e.g., 1000 ms)
        # y los agrupa por partición
        messages_by_partition = consumer.poll(timeout_ms=1000)

        if not messages_by_partition:
            # Si no hay mensajes después de 1 segundo, continua y vuelve a buscar
            print("Esperando nuevos mensajes...")
            continue

        # Si hay mensajes, los procesa
        for topic_partition, records in messages_by_partition.items():
            for message in records:
                try:
                    data = message.value
                    timestamp = data.get("timestamp")
                    source = data.get("source")
                    message_data = data.get("data")

                    # Sentencia SQL para insertar los mensajes
                    sql = f"""
                    INSERT INTO {DB_TABLE} (timestamp, source, message_data)
                    VALUES (%s, %s, %s)
                    """
                    
                    cursor.execute(sql, (timestamp, source, message_data))
                    conn.commit()
                    print(f"Mensaje insertado en PostgreSQL: {data}")

                except Exception as e:
                    print(f"Error al procesar el mensaje o insertar en la DB: {e}")
                    conn.rollback()

except Exception as e:
    print(f"Error al conectar a PostgreSQL: {e}")
    exit()

finally:
    # Cierra las conexiones al finalizar el script (en caso de error o interrupción)
    if 'consumer' in locals() and consumer:
        consumer.close()
    if 'conn' in locals() and conn:
        conn.close()