from confluent_kafka import Consumer
import os
import pandas as pd
import json
import pickle
from datetime import datetime

# Configuración del consumidor
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  
    'group.id': 'sensor_processing_group',  
    'auto.offset.reset': 'earliest',        
}
consumer = Consumer(consumer_config)

# Tópico correcto al que se está suscribiendo
consumer.subscribe(['sensor-topic'])

# Crear estructura del Data Lake
base_path = "datalake"
processed_path = os.path.join(base_path, "processed")
os.makedirs(processed_path, exist_ok=True)

# Cargar el modelo de predicción
model_filename = 'modelo_random_forest_best.pkl'
try:
    if os.path.exists(model_filename):
        with open(model_filename, 'rb') as model_file:
            model = pickle.load(model_file)
    else:
        raise FileNotFoundError(f"El modelo {model_filename} no se encontró.")
except Exception as e:
    print(f"Error al cargar el modelo: {e}")
    model = None

# Buffer para almacenar datos temporalmente
buffer = []

def parse_json_stream(data):
    """
    Divide un flujo de texto con múltiples objetos JSON concatenados.
    """
    json_objects = []
    buffer = ""
    brace_count = 0

    for char in data:
        if char == "{":
            brace_count += 1
        if char == "}":
            brace_count -= 1
        
        buffer += char
        
        if brace_count == 0 and buffer.strip():
            try:
                json_objects.append(json.loads(buffer))
            except json.JSONDecodeError as e:
                print(f"Error al decodificar JSON: {e}")
            buffer = ""

    return json_objects

def make_prediction(data):
    """
    Realiza la predicción utilizando el modelo de Random Forest.
    """
    if model:
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
            return model.predict([features])[0]  
        except Exception as e:
            print(f"Error al hacer la predicción: {e}")
            return None
    else:
        return None

def add_timestamp(record):
    """
    Añade un timestamp al registro de datos.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  
    record['timestamp_pred'] = timestamp
    return record

try:
    output_filename = os.path.join(processed_path, "sensor_data.json")
    
    while True:
        msg = consumer.poll(1.0)  
        if msg is None:
            continue 
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        # Procesar múltiples JSON concatenados
        raw_data = msg.value().decode('utf-8')
        json_records = parse_json_stream(raw_data)

        for record in json_records:
            prediction = make_prediction(record)
            record['prediction'] = prediction
            record = add_timestamp(record)
            buffer.append(record)

        # Guardar los datos acumulados como una lista JSON
        with open(output_filename, 'w') as outfile:
            json.dump(buffer, outfile, ensure_ascii=False, indent=4)
        print(f"Guardados {len(buffer)} registros en {output_filename}")

except KeyboardInterrupt:
    print("Interrumpido por el usuario.")

finally:
    consumer.close()

