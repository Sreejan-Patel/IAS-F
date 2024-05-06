from kafka import KafkaProducer
import json

# Kafka configuration
KAFKA_SERVER = "localhost:9092"
TOPIC = "logs"

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Example log message
log_message = {
    "level": 1,
    "service_name": "MyService",
    "msg": "This is an example log message"
}

# Send the log message to Kafka
producer.send(TOPIC, value=log_message)

# Close the producer
producer.close()