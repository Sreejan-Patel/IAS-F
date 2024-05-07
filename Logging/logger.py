import time
from pymongo import MongoClient
from kafka import KafkaConsumer
import json
import sys

# MongoDB Atlas URL
MONGO_KEY = "mongodb+srv://se33:se33@se3.mrwhfdo.mongodb.net/LoggerDB?retryWrites=true&w=majority&appName=Se3"

KAFKA_SERVER = "localhost:9092"
TOPIC = "logs"

def connect_to_mongodb():
    connected = False
    while not connected:
        try:
            client = MongoClient(MONGO_KEY)
            db = client["LoggerDB"]
            collection = db["loggingCollection"]
            connected = True
            return client, collection
        except Exception as e:
            print("Error connecting to MongoDB:", e)
            print("Retrying in 5 seconds...")
            time.sleep(5)

def consume_and_log():
    client, collection = connect_to_mongodb()

    # Create a Kafka consumer
    consumer = KafkaConsumer(TOPIC,
                             bootstrap_servers=KAFKA_SERVER,
                             value_deserializer=lambda m: json.loads(m.decode("utf-8")))

    for message in consumer:
        msg = message.value
        service_name = msg.get("service_name", "")
        level = msg.get("level", 1)  # Default to INFO level
        log_msg = msg.get("msg", "")

        log(client, collection, service_name, level, log_msg)

def log(client, collection, service_name, level, msg):
    levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    try:
        log_data = {
            "timestamp": time.time(),
            "service_name": service_name,
            "level": levels[level],
            "msg": msg
        }
        collection.insert_one(log_data)
        print("Log inserted successfully")
    except Exception as e:
        print("Error inserting log:", e)

# Sample Driver Code
if __name__ == "__main__":
    KAFKA_SERVER = sys.argv[-1]
    consume_and_log()