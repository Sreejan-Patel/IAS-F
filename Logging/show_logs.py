from flask import Flask, render_template
from flask_socketio import SocketIO, emit
from pymongo import MongoClient
from bson import ObjectId
import json

# MongoDB Atlas URL
MONGO_KEY = "mongodb+srv://se33:se33@se3.mrwhfdo.mongodb.net/LoggerDB?retryWrites=true&w=majority&appName=Se3"

app = Flask(__name__)
socketio = SocketIO(app)

@socketio.on('connect')
def handle_connect():
    client, collection = connect_to_mongodb()
    cursor = collection.find().sort([("timestamp", 1)])
    for document in cursor:
        document["_id"] = str(document["_id"])
        emit('new_log', document)

def connect_to_mongodb():
    client = MongoClient(MONGO_KEY)
    db = client["LoggerDB"]
    collection = db["loggingCollection"]
    return client, collection

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == "__main__":
    socketio.run(app, debug=True)