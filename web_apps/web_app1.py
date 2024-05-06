from flask import Flask, request, jsonify
import numpy as np
import tensorflow as tf
import sys
import requests
from flask_cors import CORS

app = Flask(__name__)
cors = CORS(app)

@app.route('/')
def index():
    return app.send_static_file('upload.html')

@app.route('/upload', methods=['POST'])
def upload():
    print('start of upload()')
    if 'image' in request.files:
        print('inside if')
        print(request.files)
        # Get the image file from the request
        image_file = request.files['image']
        print('image_file', image_file)
        print('Received image:', image_file.filename)
        # Forward the image to the /receive_input route
        response = requests.post('http://127.0.0.1:7000/receive_input', files={'image': image_file})

        # Check ifffffffFFFFFFF the request was successful
        if response.status_code == 200:
            return jsonify({'message': 'Data received and sent to Kafka successfully'})
        else:
            return jsonify({'message': 'Failed to send data to Kafka'}), response.status_code
    else:
        print('inside else')
        return jsonify({'message': 'No image uploaded'}), 400

if __name__ == '__main__':
    app.run(debug=False,port=1234)
