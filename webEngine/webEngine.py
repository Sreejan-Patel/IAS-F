''' 
Developer designs an app with components: 
- Webapp (takes input from end user)
- Inference (takes input from webapp and returns output)

Our task is to develop a webEngine that takes input from webapp, converts it to
kafka message, and sends it to inference engine. Inference engine will convert 
it back to a HTTP request and send it to the inference service. 

The developer is required to send the HTTP request from the webapp via a URL
which is taken as an argument (we will provide our webEngine URL as an argument
during deployment). 
Similarly, the inference engine will send the HTTP request to the inference
service via a URL which is taken as an argument.  
'''
from kafka import KafkaProducer, KafkaConsumer
import json
import requests
from flask import Flask, request,render_template

import base64
from flask_cors import CORS

import threading
import time
import sys

# def img_to_json(img):
    
app = Flask(__name__)
cors = CORS(app)

BOOTSTRAP_SERVER = "localhost:9092"

'''
The endpoint url is addded to the HTTP json request (field is destination). 
Instead of endpoint URL, the url field is set to webEngine URL.
destination: URL
url: webEngine

If this doesn't work, use nginx as proxy server to redirect all URLs to webEngine
'''

# destination_url = 'http://127.0.0.1:6003/display'
@app.route('/receive_input', methods=['POST'])
def receive_input():

    logProducer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    log_message = {
        "level": 0,
        "service_name": "webEngine",
        "msg": "Received input from webapp"
    }
    logProducer.send('logs', value=log_message)
    t_stamp = time.time()
    if len(request.files) == 0:
        log_message = {
            "level": 3,
            "service_name": "webEngine",
            "msg": "No data received"
        }
        logProducer.send('logs', value=log_message)
        return "No data received", 400

    # Iterate through each file in the request
    for file_key, file_obj in request.files.items():
        # Determine the type of the file
        # print('file_key: ', file_key, file_obj.filename, file_obj)
        file_type = determine_file_type(file_obj.filename)
        # print('file_type: ', file_type)
        log_message = {
            "level": 0,
            "service_name": "webEngine",
            "msg": "Data sent to Kafka - input topic"
        }
        logProducer.send('logs', value=log_message)
        # Process the file based on its type
        if file_type == 'image':
            # print('before process_image')
            process_image(file_obj,t_stamp)
        elif file_type == 'audio':
            process_audio(file_obj)
        elif file_type == 'text':
            process_text(file_obj)
        else:
            # Unsupported file type
            print(file_type)
            return f"Unsupported file type: {file_type}", 400

    predicted = None
    consumer = KafkaConsumer('output', bootstrap_servers=BOOTSTRAP_SERVER,auto_offset_reset='earliest')
    # print("start of kafka_consumer()")
    for message in consumer:
        #print("message: ",message)
        try:
            #print("entering try block")
            data = json.loads(message.value.decode('utf-8'))
            if data["tstamp"] == t_stamp:
                # print(data["data"])
                log_message = {
                    "level": 0,
                    "service_name": "webEngine",
                    "msg": "Data received from Kafka - output topic"
                }
                logProducer.send('logs', value=log_message)
                predicted = data["data"]
                break
        except KeyError:
            pass
        except Exception as e:
            print("Error processing message:", str(e))
        
        
    consumer.close()   
    
    
    return predicted, 200

def determine_file_type(filename):
    print('start of determine_file_type()', filename)
    # Simple logic to determine the file type based on the file extension
    if filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif')):
        return 'image'
    elif filename.lower().endswith(('.wav', '.mp3', '.ogg')):
        return 'audio'
    elif filename.lower().endswith(('.txt', '.csv')):
        return 'text'
    else:
        return 'unknown'

def process_image(image_file,t_stamp):
    print('start of process_image()')
    # Read the image data and encode it as base64
    image_data = image_file.read()
    base64_image = base64.b64encode(image_data).decode('utf-8')

    print('before kafka sending')
    # Send the base64 encoded image data to Kafka
    request = {"data": base64_image,"tstamp":t_stamp}
    kafka_producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    kafka_producer.send("input", value=json.dumps(request).encode('utf-8'))
    print('before flush')
    kafka_producer.flush()

    print("Image data sent to Kafka successfully")

def process_audio(audio_file):
    # Read the audio data and encode it as base64
    audio_data = audio_file.read()
    base64_audio = base64.b64encode(audio_data).decode('utf-8')
    kafka_producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    # Send the base64 encoded audio data to Kafka
    kafka_producer.send("input", value=base64_audio.encode('utf-8'))
    kafka_producer.flush()

    print("Audio data sent to Kafka successfully")

def process_text(text_file):
    # Read the text data
    text_data = text_file.read().decode('utf-8')
    kafka_producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    # Send the text data to Kafka
    kafka_producer.send("input", value=text_data.encode('utf-8'))
    kafka_producer.flush()

    print("Text data sent to Kafka successfully")

def send_output_to_url(data):
    print('start of send_output_to_url()', data)
    try:
        print("before sending data to URL ", data)
        response = requests.post(destination_url, json=data)
        return response
    except: 
        print("Error sending data to URL")
        return
    




# def kafka_consumer():
#     consumer = KafkaConsumer('output', bootstrap_servers=BOOTSTRAP_SERVER)
#     print("start of kafka_consumer()")
#     # while True:
#         # print('here')
#     global globaldata
#     globaldata = None
#     for message in consumer:
#         print("message: ",message)
#         try:
#             print("entering try block")
#             globaldata = json.loads(message.value.decode('utf-8'))
#             print('MESSAGE VALUE: ', message.value.decode('utf-8'))
#             print("before send_output_to_url", globaldata)
#             return globaldata
#         except Exception as e:
#             print("Error processing message:", str(e))
            
#     consumer.close()
# Example usage:
if __name__ == "__main__":
    BOOTSTRAP_SERVER = sys.argv[-1]
    logProducer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    log_message = {
        "level": 0,
        "service_name": "webEngine",
        "msg": "Starting webEngine service"
    }
    logProducer.send('logs', value=log_message)

    app.run(port=7000)
    



