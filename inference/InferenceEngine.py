from kafka import KafkaProducer, KafkaConsumer
import json
import requests
from flask import Flask, request, jsonify
from flask_cors import CORS
import sys

app = Flask(__name__)
cors = CORS(app)

BOOTSTRAP_SERVER = "localhost:9092"
URL = 'http://127.0.0.1:6003/predict'

@app.route('/receive_output', methods=['POST'])
def receive_output():
    # print('start of receive_output()')

    logProducer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    log_message = {
        "level": "0",
        "service_name": "inferenceEngine",
        "msg": "Received output from inference service"
    }
    logProducer.send('logs', value=log_message)
    kafka_producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    data = request.json
    # print("Received data:", data, type(data))    
    # Check if data is received
    if data is not None:
        print("before send")
        kafka_producer.send("output", value=data)
        print("receive_output() before flush", data)
        kafka_producer.flush()
        log_message = {
            "level": "0",
            "service_name": "inferenceEngine",
            "msg": "Data sent to Kafka successfully - output topic"
        }
        logProducer.send('logs', value=log_message)
        print('after flush')
        return "Data received and sent to Kafka successfully receive_output()", 200
    else:
        log_message = {
            "level": "3",
            "service_name": "inferenceEngine",
            "msg": "No data received"
        }
        logProducer.send('logs', value=log_message)
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
    # print("start of kafka_consumer()")
    logProducer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    consumer = KafkaConsumer('input', bootstrap_servers=BOOTSTRAP_SERVER)
    for message in consumer:
        # print("message: ",message)
        try:
            url = URL
            data = message.value     
            # print('before send_input_to_url') 
            log_message = {
                "level": "0",
                "service_name": "inferenceEngine",
                "msg": "Received input from input topic"
            }
            logProducer.send('logs', value=log_message)
            send_input_to_url(url, data)
        except Exception as e:
            print("Error processing message:", str(e))

if __name__ == "__main__":
    # print("Starting Kafka consumer thread")
    BOOTSTRAP_SERVER = sys.argv[-1]
    import threading
    consumer_thread = threading.Thread(target=kafka_consumer)
    consumer_thread.start()
    
    # Must listen to 
    app.run(port=6005)
    

