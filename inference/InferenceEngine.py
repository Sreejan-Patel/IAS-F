from kafka import KafkaProducer, KafkaConsumer
import json
import requests
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
cors = CORS(app)

kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
URL = 'http://127.0.0.1:6001/predict'

@app.route('/receive_output', methods=['POST'])
def receive_output():
    print('start of receive_output()')
    
    data = request.json
    print("Received data:", data, type(data))    
    # Check if data is received
    if data is not None:
        print("before send")
        kafka_producer.send("output", value=data)
        print("receive_output() before flush")
        kafka_producer.flush()
        print('after flush')
        return "Data received and sent to Kafka successfully receive_output()", 200
    else:
        return "No data received", 400 

def send_input_to_url(url, data):
    print('start of send_input_to_url()', data)
    try:
        response = requests.post(url=url, json=data)
        if response.status_code == 200:
            print("Data sent successfully to URL")
        else:
            print("Failed to send data to URL:", response.status_code)
    except Exception as e:
        print("Exception occurred while sending data to URL:", str(e))
def kafka_consumer():
    print("start of kafka_consumer()")
    consumer = KafkaConsumer('input', bootstrap_servers='localhost:9092')
    for message in consumer:
        print("message: ",message)
        try:
            url = URL
            data = message.value     
            print('before send_input_to_url')                  
            send_input_to_url(url, data)
        except Exception as e:
            print("Error processing message:", str(e))

if __name__ == "__main__":
    print("Starting Kafka consumer thread")
    import threading
    consumer_thread = threading.Thread(target=kafka_consumer)
    consumer_thread.start()
    print("nibba rohit")
    
    # Must listen to 
    app.run(port=6000)
    

