import os
import json
import requests
from kafka import KafkaProducer

# Create directories if they don't exist
os.makedirs('datalake/raw', exist_ok=True)

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# URL of the Flask server endpoint
flask_server_url = 'http://localhost:5000/get_element'

def fetch_and_send_data():
    try:
        # Fetch data from the Flask server
        response = requests.get(flask_server_url)
        response.raise_for_status()
        data = response.json()

        # Save the data to a file in the datalake/producer folder
        file_path = os.path.join('datalake/producer', 'data.json')
        with open(file_path, 'w') as file:
            json.dump(data, file)

        # Send the data to the Kafka topic
        producer.send('sensor-topic', data)
        producer.flush()
        print("Data sent to Kafka topic 'sensor-topic' and saved to datalake/raw/data.json")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Flask server: {e}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fetch_and_send_data()