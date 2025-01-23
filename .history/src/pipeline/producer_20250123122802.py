import json
import requests
from confluent_kafka import Producer
import threading
import time

# Configuración del productor de Kafka
producer_config = {
    'bootstrap.servers': 'localhost:9092',  
    'client.id': 'sensor-producer',         
}
producer = Producer(producer_config)

flask_server_url = 'http://localhost:5000/get_element'

def delivery_report(err, msg):
    """
    Callback para confirmar la entrega del mensaje.
    """
    if err is not None:
        print(f"Error al enviar mensaje: {err}")
    else:
        print(f"Mensaje enviado correctamente a {msg.topic()} [partición {msg.partition()}]")

def fetch_and_send_data():
    """
    Obtiene los datos del servidor Flask y los envía al productor Kafka periódicamente.
    """
    while True:
        try:
            response = requests.get(flask_server_url)
            response.raise_for_status()  
            data = response.json()

            producer.produce(
                topic='sensor-topic', 
                value=json.dumps(data),
                callback=delivery_report
            )
            producer.flush()  
            print("Datos enviados al tópico 'sensor-topic'")

        except requests.exceptions.RequestException as e:
            print(f"Error al obtener datos del servidor Flask: {e}")
        except Exception as e:
            print(f"Error inesperado: {e}")

        time.sleep(5)  

if __name__ == "__main__":
    data_thread = threading.Thread(target=fetch_and_send_data, daemon=True)
    data_thread.start()

    input("Presiona Enter para detener el productor Kafka...\n")
