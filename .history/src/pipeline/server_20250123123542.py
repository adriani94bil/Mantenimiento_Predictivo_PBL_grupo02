from flask import Flask, jsonify
import os
import json
import threading
import time

app = Flask(__name__)

# Crear la carpeta datalake/raw si no existe
os.makedirs('datalake/raw', exist_ok=True)

# Cargar los datos desde feed.json
feed_data_path = 'feed.json'
if not os.path.exists(feed_data_path):
    raise FileNotFoundError(f"No se encontró el archivo {feed_data_path}")

with open(feed_data_path, 'r') as f:
    feed_data = json.load(f)

# Índice actual para enviar datos secuenciales
current_index = 0

def save_data_to_raw(data):
    """
    Guarda los datos en la carpeta datalake/raw agregando elementos a un archivo JSON.
    """
    file_path = os.path.join('datalake/raw', 'data.json')

    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            existing_data = json.load(file)
    else:
        existing_data = []

    existing_data.append(data)

    with open(file_path, 'w') as file:
        json.dump(existing_data, file, indent=4)

    print(f"Datos agregados en {file_path}")

def send_element():
    """
    Simula el envío periódico de datos y los guarda en datalake/raw.
    """
    global current_index
    while True:
        if current_index < len(feed_data):
            element = feed_data[current_index]
            current_index += 1

            save_data_to_raw(element)

            print(f"Preparando elemento para el productor Kafka: {element}")
        else:
            current_index = 0 
        time.sleep(5)  

@app.route('/get_element', methods=['GET'])
def get_element():
    """
    Devuelve el elemento actual al cliente (productor Kafka).
    """
    global current_index
    if current_index < len(feed_data):
        element = feed_data[current_index]
        return jsonify(element)
    else:
        return jsonify({"error": "No hay más elementos"}), 404

if __name__ == '__main__':
    threading.Thread(target=send_element, daemon=True).start()
    app.run(host='0.0.0.0', port=5000)
