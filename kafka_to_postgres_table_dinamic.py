import os
import json
import time
import psycopg2
import re 
from kafka import KafkaConsumer
# Configuración de variables de entorno
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC") 

# Almacena las tablas que ya han sido creadas para evitar consultas repetidas
CREATED_TABLES = set()

def create_table_if_not_exists(cursor, table_name, data):
    """
    Crea una tabla en PostgreSQL si no existe, basándose en la estructura del mensaje.
    """
    # Si la tabla ya está en el set de tablas creadas, salta la verificación
    if table_name in CREATED_TABLES:
        return

    try:
        # Verifica si la tabla ya existe en la base de datos
        cursor.execute("SELECT 1 FROM information_schema.tables WHERE table_name = %s;", (table_name,))
        if cursor.fetchone():
            print(f"La tabla '{table_name}' ya existe.")
            CREATED_TABLES.add(table_name)
            return

        print(f"Creando la tabla '{table_name}' dinámicamente...")
        
        # Construye las columnas de la tabla a partir de las claves del mensaje
        columns = []
        for key, value in data.items():
            column_name = key
            # Elige el tipo de dato de PostgreSQL basado en el tipo de Python
            if isinstance(value, int) or isinstance(value, float):
                column_type = "numeric"
            elif isinstance(value, str):
                column_type = "text"
            else:
                column_type = "text" # Valor por defecto
            
            columns.append(f"{column_name} {column_type}")
        
        # Genera y ejecuta la sentencia CREATE TABLE
        columns_sql = ", ".join(columns)
        create_table_sql = f"CREATE TABLE {table_name} ({columns_sql});"
        cursor.execute(create_table_sql)
        conn.commit()
        CREATED_TABLES.add(table_name)
        print(f"Tabla '{table_name}' creada exitosamente.")

        # Convierte la tabla recién creada en una hipertabla
        print(f"Convirtiendo la tabla '{table_name}' en una hipertabla...")
        cursor.execute(f"SELECT create_hypertable('{table_name}', 'timestamp');")
        conn.commit()
        CREATED_TABLES.add(table_name)
        print(f"Tabla '{table_name}' convertida a hipertabla exitosamente.")

    except psycopg2.Error as e:
        # En caso de error, como una tabla creada por otro consumidor,
        # simplemente se ignora y se sigue adelante.
        print(f"Error al intentar crear la tabla '{table_name}': {e}")
        conn.rollback()

# Conexión a Kafka
try:
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-postgres-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    # Suscribe al consumidor a todos los tópicos que comiencen con 'plc-data-'
    
    consumer.subscribe(pattern=re.compile(f"{KAFKA_TOPIC}.*"))
    print("Conexión a Kafka exitosa. Escuchando todos los tópicos que coinciden con el patrón 'plc-data-.*'.")

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
    
    # Bucle principal para escuchar y procesar mensajes
    for message in consumer:
        try:
            # 1. Obtiene el nombre del tópico y lo usa como nombre de tabla
            table_name = message.topic
            
            # 2. Obtiene los datos del mensaje
            data = message.value
            
            # 3. y 4. Llama a la función para crear la tabla si no existe
            create_table_if_not_exists(cursor, table_name, data)
            
            # 5. Prepara y ejecuta la inserción de datos
            columns = list(data.keys())
            values = list(data.values())
            
            # Usa %s para evitar inyección SQL
            columns_sql = ", ".join(columns)
            values_placeholders = ", ".join(["%s"] * len(values))
            
            sql = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({values_placeholders});"
            cursor.execute(sql, values)
            conn.commit()
            print(f"Mensaje insertado en la tabla '{table_name}': {data}")

        except Exception as e:
            print(f"Error al procesar el mensaje o insertar en la DB: {e}")
            conn.rollback()

except Exception as e:
    print(f"Error al conectar a PostgreSQL: {e}")
    exit()

finally:
    if 'consumer' in locals() and consumer:
        consumer.close()
    if 'conn' in locals() and conn:
        conn.close()