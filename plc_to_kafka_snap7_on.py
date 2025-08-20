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
POLLING_INTERVAL = int(os.getenv("POLLING_INTERVAL", 1)) # Intervalo de lectura en segundos

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

# Bucle principal de lectura y envío de datos
try:
    plc = snap7.client.Client()
    plc.connect(PLC_IP, PLC_RACK, PLC_SLOT)
    
    if plc.get_connected():
        print(f"Conexión a PLC Siemens en {PLC_IP} exitosa.")
    else:
        print(f"Error: No se pudo conectar al PLC en {PLC_IP}.")
        exit()

    while True:
        # Aquí debes implementar la lógica para leer los tags de tu PLC
        # Por ejemplo, para leer 100 bytes del Data Block (DB) 1:
        # data_block = plc.db_read(1, 0, 100)
        # Puedes usar snap7.util para convertir los bytes a valores.
        
        # Ejemplo con datos simulados para ilustrar el formato:
        plc_data = {
            "timestamp": int(time.time() * 1000),
            "source": PLC_IP,
            "data": "valor_real_del_plc" 
        }

        # Envía el mensaje al tópico de Kafka
        producer.send(KAFKA_TOPIC, value=plc_data)
        print(f"Mensaje enviado a Kafka: {plc_data}")

        # Espera el intervalo de tiempo antes de la siguiente lectura
        time.sleep(POLLING_INTERVAL)

except snap7.snap7.common.S7DriverError as e:
    print(f"Error de Snap7 al conectar/leer del PLC: {e}")
except Exception as e:
    print(f"Error en el bucle principal o al conectar/leer del PLC: {e}")
finally:
    # Cierra las conexiones del PLC y del productor de Kafka
    if plc is not None and plc.get_connected():
        plc.disconnect()
    if producer is not None:
        producer.flush()
        producer.close()
    print("Script finalizado.")