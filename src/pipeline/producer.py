import json
import requests
from confluent_kafka import Producer
import threading
import time

# Configuración del productor de Kafka
producer_config = {
    'bootstrap.servers': 'localhost:9092',  # Servidor de Kafka
    'client.id': 'sensor-producer',         # ID del cliente
}
producer = Producer(producer_config)

# URL del endpoint del servidor Flask
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
            # Obtener datos del servidor Flask
            response = requests.get(flask_server_url)
            response.raise_for_status()  # Verifica si hubo algún error en la respuesta
            data = response.json()

            # Enviar los datos al tópico de Kafka
            producer.produce(
                topic='sensor-topic', 
                value=json.dumps(data),
                callback=delivery_report
            )
            producer.flush()  # Asegurarse de que se envíen todos los mensajes
            print("Datos enviados al tópico 'sensor-topic'")

        except requests.exceptions.RequestException as e:
            print(f"Error al obtener datos del servidor Flask: {e}")
        except Exception as e:
            print(f"Error inesperado: {e}")

        time.sleep(5)  # Esperar 5 segundos antes de obtener el siguiente conjunto de datos

if __name__ == "__main__":
    # Iniciar el hilo que obtiene y envía datos de manera continua
    data_thread = threading.Thread(target=fetch_and_send_data, daemon=True)
    data_thread.start()

    # Mantener el hilo principal activo para que Flask también siga ejecutándose
    input("Presiona Enter para detener el productor Kafka...\n")
