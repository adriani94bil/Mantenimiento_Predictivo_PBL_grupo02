from confluent_kafka import Consumer
import os
import pandas as pd
import json
import pickle
from datetime import datetime

# Configuración del consumidor
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Servidor Kafka
    'group.id': 'sensor_processing_group',  # Grupo de consumidores
    'auto.offset.reset': 'earliest',        # Empieza desde el principio si es la primera vez
}
consumer = Consumer(consumer_config)

# Tópico correcto al que se está suscribiendo
consumer.subscribe(['sensor-topic'])  # Cambiado para que coincida con el productor

# Crear estructura del Data Lake
base_path = "datalake"
processed_path = os.path.join(base_path, "processed")
os.makedirs(processed_path, exist_ok=True)  # Crear carpeta processed si no existe

# Cargar el modelo de predicción (Random Forest)
model_filename = 'modelo_random_forest_best.pkl'

try:
    if os.path.exists(model_filename):
        with open(model_filename, 'rb') as model_file:
            model = pickle.load(model_file)
    else:
        raise FileNotFoundError(f"El modelo {model_filename} no se encontró. Asegúrate de que el archivo exista.")
except Exception as e:
    print(f"Error al cargar el modelo: {e}")
    model = None

# Buffer para almacenar datos temporalmente
buffer = []

def make_prediction(data):
    """
    Realiza la predicción utilizando el modelo de Random Forest.
    """
    if model:
        # Extraer todas las características del registro de forma dinámica
        features = [
            data.get('S1_mean', 0),
            data.get('S1_median', 0),
            data.get('S1_std', 0),
            data.get('S1_cv', 0),
            data.get('S1_min', 0),
            data.get('S1_IQR', 0),
            data.get('S2_mean', 0),
            data.get('S2_median', 0),
            data.get('S2_std', 0),
            data.get('S2_cv', 0),
            data.get('S3_mean', 0),
            data.get('S3_std', 0),
            data.get('S3_var', 0),
            data.get('S3_cv', 0),
            data.get('S4_mean', 0),
            data.get('S4_median', 0),
            data.get('S4_std', 0),
            data.get('S4_cv', 0),
            data.get('S5_mean', 0),
            data.get('S5_std', 0),
            data.get('S5_cv', 0),
            data.get('S5_min', 0),
            data.get('S5_max', 0),
            data.get('S6_mean', 0),
            data.get('S6_std', 0),
            data.get('S6_cv', 0),
            data.get('S7_mean', 0),
            data.get('S7_std', 0),
            data.get('S7_cv', 0),
            data.get('S8_std', 0),
            data.get('S8_cv', 0),
            data.get('S8_max', 0)
        ]

        try:
            # Realizar la predicción utilizando todas las características extraídas
            return model.predict([features])[0]  # El índice [0] para obtener el valor de la predicción
        except Exception as e:
            print(f"Error al hacer la predicción: {e}")
            return None
    else:
        return None

def add_timestamp(record):
    """
    Añade un timestamp al registro de datos.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Formato de fecha y hora
    record['timestamp_pred'] = timestamp
    return record

try:
    while True:
        # Poll para obtener mensajes
        msg = consumer.poll(1.0)  # Espera 1 segundo por un mensaje
        if msg is None:
            continue  # No hay mensajes nuevos
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        # Procesar el mensaje recibido
        record = json.loads(msg.value().decode('utf-8'))
        
        # Convertir el registro en un DataFrame (aunque no es estrictamente necesario)
        df_record = pd.DataFrame([record])

        # Realizar la predicción con el modelo
        prediction = make_prediction(record)

        # Añadir la predicción al registro de datos
        record['prediction'] = prediction

        # Añadir el timestamp al registro de datos
        record = add_timestamp(record)

        # Añadir el registro al buffer
        buffer.append(record)

        # Guardar datos cada 5 registros
        if len(buffer) >= 5:
            # Guardar los datos procesados en un archivo JSON
            output_filename = os.path.join(processed_path, f"sensor_data_{len(buffer)}.json")
            with open(output_filename, 'w') as outfile:
                json.dump(buffer, outfile, indent=4)
            print(f"Guardados {len(buffer)} registros en {output_filename}")
            buffer.clear()

except KeyboardInterrupt:
    print("Interrumpido por el usuario.")

finally:
    # Guardar datos restantes en el buffer
    if buffer:
        output_filename = os.path.join(processed_path, f"sensor_data_remaining.json")
        with open(output_filename, 'w') as outfile:
            json.dump(buffer, outfile, indent=4)
        print(f"Guardados {len(buffer)} registros restantes en {output_filename}")

    # Cerrar el consumidor de Kafka
    consumer.close()
