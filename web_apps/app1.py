from flask import Flask, request, render_template
import numpy as np
import tensorflow as tf
import sys
import os
import requests

# mnist model import
from keras.datasets import mnist
from keras.models import load_model
from flask_cors import CORS
from PIL import Image
from io import BytesIO
import json

import base64

app = Flask(__name__)
cors = CORS(app)

from flask import jsonify
from tensorflow.keras.models import load_model


# URL is taken as an argument
URL = '/predict'
url_infer = 'http://127.0.0.1:6005/receive_output'

model = tf.keras.models.Sequential([
    tf.keras.layers.Flatten(input_shape=(28, 28)),
    tf.keras.layers.Dense(128, activation='softmax'),
    tf.keras.layers.Dense(10)
    ])

# Define a function to preprocess input image
def preprocess_image(image):
    # Reshape the image to 28x28 and normalize pixel values
    print('inside preprocess_image')
    
    image = np.array(image, dtype=np.uint8)
    print('before reshape', image.shape)
    # image = image.reshape((1, 28, 28, 1))
    # image /= 255.0
    # return image
    image_resized = Image.fromarray(image).resize((28, 28))
    # Convert the image to grayscale
    image_gray = image_resized.convert('L')
    # Normalize pixel values
    image_array = np.array(image_gray, dtype=np.float32).reshape((1, 28, 28, 1)) / 255.0
    return image_array

@app.route('/predict', methods=['POST'])
def predict():
    # Get image data from request
    print('request: ',request.data)
    print('start of predict2(): ', request.json)
    req = json.loads(request.json)
    print("request",req)
    
    # convert "b'encoded_image'" to image
    
    decoded_image = base64.b64decode(req['data'])
    image = Image.open(BytesIO(decoded_image))
    print('after decodng')
    
    # Make prediction
    image = preprocess_image(image)
    prediction = model(image)
    print('after prediction', prediction)
    
    # Get the predicted digit
    predicted_digit = np.argmax(prediction)
    print('predicted_digit', predicted_digit)
    
    # prepare the response (json)
    data = {'predicted_digit': str(predicted_digit)}
    req['data'] = str(predicted_digit)
    print("request.data")
    print('before send_output: ', req)
    send_output(url_infer,req)
    print('after send_output')
    return req
    
def send_output(url,data):
    print('inside send_output')
    try:
        print('inside try, send_output')
        response = requests.post(url, json=data)
        if response.status_code == 200:
            print("Data sent successfully to URL")
        else:
            print("Failed to send data to URL:", response.status_code)
    except Exception as e:
        print("Exception occurred while sending data to URL:", str(e))


@app.route('/display', methods=['POST'])
def display():    
    print('start of display, ', request)  
    print(request.json)
    
    # incomplete - send this data to frontend
    return request.json

if __name__ == '__main__':
    # check if model is pre-trained and saved ('MNIST_MODEL.keras')  
    if not os.path.exists('MNIST_MODEL.keras'):  
        print('training model')
        # Train the MNIST model
        (train_X, train_y), (test_X, test_y) = mnist.load_data()

        ds_train = tf.data.Dataset.from_tensor_slices((train_X, train_y))
        ds_train = ds_train.shuffle(60050).batch(32)
        ds_test = tf.data.Dataset.from_tensor_slices((test_X, test_y))
        ds_test = ds_test.batch(32)

        model.compile(
            optimizer=tf.keras.optimizers.Adam(0.001),
            loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
            metrics=[tf.keras.metrics.SparseCategoricalAccuracy()],
        )
        
        model.fit(
            ds_train,
            epochs=30,
            validation_data=ds_test,
        )

        model.save('MNIST_MODEL.keras')
        print("model saved to disk")
    
    print('loading model')
    model = load_model('MNIST_MODEL.keras')
    
    
    app.run(debug=True, port=6003)
    
