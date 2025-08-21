import os
import json
import time
import snap7
from kafka import KafkaProducer

# Configuración de variables de entorno
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
PLC_IP = os.getenv("PLC_IP")
PLC_RACK = int(os.getenv("PLC_RACK"))
PLC_SLOT = int(os.getenv("PLC_SLOT"))

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
    plc = snap7.client.Client()
    plc.connect(PLC_IP, PLC_RACK, PLC_SLOT)
    
    if plc.get_connected():
        print(f"Conexión a PLC Siemens en {PLC_IP} exitosa.")
        
        # Aquí debes implementar la lógica para leer los tags de tu PLC
        # y guardar los datos en un diccionario
        # Ejemplo con datos simulados:
        plc_data = {
            "timestamp": int(time.time() * 1000),
            "source": PLC_IP,
            "data": "valor_real_del_plc" 
        }

        # Envía el mensaje al tópico de Kafka
        producer.send(KAFKA_TOPIC, value=plc_data)
        print(f"Mensaje enviado a Kafka: {plc_data}")
        
    else:
        print(f"Error: No se pudo conectar al PLC en {PLC_IP}.")

except RuntimeError as e:
    print(f"Error de Snap7 al conectar/leer del PLC: {e}")
except Exception as e:
    print(f"Error al procesar el mensaje o al conectar al PLC: {e}")
finally:
    # Cierra las conexiones del PLC y del productor de Kafka
    if plc is not None and plc.get_connected():
        plc.disconnect()
    if producer is not None:
        producer.flush()
        producer.close()
    print("Script finalizado.")