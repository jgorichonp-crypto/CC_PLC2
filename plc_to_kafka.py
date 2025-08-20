import os
from kafka import KafkaProducer
import json
import time

# Configuración de variables de entorno
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
MENSAJE = os.getenv("MENSAJE")

# El mensaje que quieres enviar a Kafka
# Puedes cambiar este diccionario por cualquier JSON que necesites.
message = {
    "timestamp": time.time(),
    "source": "simulacion",
    "data": MENSAJE
}

try:
    # Conexión al productor de Kafka
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Conexión a Kafka exitosa.")
    print(KAFKA_BROKER)
    print(KAFKA_TOPIC)

    # Envía el mensaje al topic
    producer.send(KAFKA_TOPIC, value=message)
    print(f"Mensaje enviado a Kafka: {message}")

    # Asegura que el mensaje se envíe antes de salir
    producer.flush()
    print("Proceso completado. Saliendo.")

except Exception as e:
    print(f"Error al conectar o enviar a Kafka: {e}")