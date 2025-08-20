import os
import json
import time
from kafka import KafkaProducer
from plc_driver import PLC # Placeholder para tu librería de PLC

# Configuración de variables de entorno
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
PLC_IP = os.getenv("PLC_IP")

# Configuración de la conexión a Kafka
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Conexión a Kafka exitosa.")
except Exception as e:
    print(f"Error al conectar a Kafka: {e}")
    exit()

# Lógica de conexión, lectura y envío de datos
try:
    # Este es un placeholder, reemplázalo con tu lógica de conexión
    plc = PLC(ip=PLC_IP) 
    print(f"Conexión a PLC en {PLC_IP} exitosa.")
    
    # Aquí debes implementar la lógica para leer los datos de tu PLC
    # y guardar los datos en un diccionario.
    # Ejemplo con datos simulados:
    plc_data = {
        "timestamp": int(time.time() * 1000),
        "source": PLC_IP,
        "data": "valor_real_del_plc" 
    }

    # Envía el mensaje al tópico de Kafka
    producer.send(KAFKA_TOPIC, value=plc_data)
    print(f"Mensaje enviado a Kafka: {plc_data}")

except Exception as e:
    print(f"Error al procesar el mensaje o al conectar al PLC: {e}")
finally:
    # Cierra la conexión del productor de Kafka
    if producer is not None:
        producer.flush()
        producer.close()
    print("Script finalizado.")