from flask import Flask, jsonify
import json
import threading
import time

app = Flask(__name__)

# Load the feed.json file
with open(r'src/pipeline/feed.json', 'r') as f:
    feed_data = json.load(f)

# Index to keep track of the current element
current_index = 0

def send_element():
    global current_index
    while True:
        if current_index < len(feed_data):
            element = feed_data[current_index]
            current_index += 1
            # Here you would send the element to the Kafka producer
            print(f"Sending element to Kafka producer: {element}")
        else:
            current_index = 0
        time.sleep(5)

@app.route('/get_element', methods=['GET'])
def get_element():
    global current_index
    if current_index < len(feed_data):
        element = feed_data[current_index]
        return jsonify(element)
    else:
        return jsonify({"error": "No more elements"}), 404

if __name__ == '__main__':
    # Start the thread to send elements every 5 seconds
    threading.Thread(target=send_element, daemon=True).start()
    app.run(host='0.0.0.0', port=5000)