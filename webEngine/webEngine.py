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

# def img_to_json(img):
    
app = Flask(__name__)
cors = CORS(app)

import logbook
logbook.set_datetime_format("local")

# import sys
# sys.path.append('..')
# from logs.logger import setup_logger

'''
The endpoint url is addded to the HTTP json request (field is destination). 
Instead of endpoint URL, the url field is set to webEngine URL.
destination: URL
url: webEngine

If this doesn't work, use nginx as proxy server to redirect all URLs to webEngine
'''

# destination_url = 'http://127.0.0.1:6001/display'
kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092')
@app.route('/receive_input', methods=['POST'])
def receive_input():
    print('start of receive_input()')
   
    if len(request.files) == 0:
        return "No data received", 400

    # Iterate through each file in the request
    print('before for', request.files['image'].filename)
    print(request)
    for file_key, file_obj in request.files.items():
        # Determine the type of the file
        print('file_key: ', file_key, file_obj.filename, file_obj)
        file_type = determine_file_type(file_obj.filename)
        print('file_type: ', file_type)

        # Process the file based on its type
        if file_type == 'image':
            print('before process_image')
            process_image(file_obj)
        elif file_type == 'audio':
            process_audio(file_obj)
        elif file_type == 'text':
            process_text(file_obj)
        else:
            # Unsupported file type
            print(file_type)
            return f"Unsupported file type: {file_type}", 400
        
        print('before thread')
        consumer_thread.join()
        print('after join', globaldata)

        return globaldata, 200

    print('end of receive_input()')

    return "Data received and sent to Kafka successfully", 200

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

def process_image(image_file):
    print('start of process_image()')
    # Read the image data and encode it as base64
    image_data = image_file.read()
    base64_image = base64.b64encode(image_data).decode('utf-8')

    print('before kafka sending')
    # Send the base64 encoded image data to Kafka
    kafka_producer.send("input", value=base64_image.encode('utf-8'))
    print('before flush')
    kafka_producer.flush()

    print("Image data sent to Kafka successfully")

def process_audio(audio_file):
    # Read the audio data and encode it as base64
    audio_data = audio_file.read()
    base64_audio = base64.b64encode(audio_data).decode('utf-8')

    # Send the base64 encoded audio data to Kafka
    kafka_producer.send("input", value=base64_audio.encode('utf-8'))
    kafka_producer.flush()

    print("Audio data sent to Kafka successfully")

def process_text(text_file):
    # Read the text data
    text_data = text_file.read().decode('utf-8')

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
    
        
def kafka_consumer():
    consumer = KafkaConsumer('output', bootstrap_servers='localhost:9092')
    print("start of kafka_consumer()")
    # while True:
        # print('here')
    for message in consumer:
        print("message: ",message)
        try:
            global globaldata
            print("entering try block")
            globaldata = json.loads(message.value.decode('utf-8'))
            print("before send_output_to_url", globaldata)
            # send_output_to_url(data)
            # print("end")
            return globaldata
        except Exception as e:
            print("Error processing message:", str(e))

# Example usage:
if __name__ == "__main__":
    # logbook
    # logger = setup_logger("../central_log.log")
    # print("WebEngine started")
    # logger.info("WebEngine started 2")
    # logger.info("WebEngine started 3")

    consumer_thread = threading.Thread(target=kafka_consumer)
    consumer_thread.start()
    app.run(port=7000)
    