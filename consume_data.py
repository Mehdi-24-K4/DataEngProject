from confluent_kafka import Consumer, KafkaError
import json
from pymongo import MongoClient

# Configuration du consommateur
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'air_quality_consumer_group',
    'auto.offset.reset': 'earliest'
}

# Créer le consommateur
consumer = Consumer(conf)

# S'abonner au topic
topic = 'air_quality_data'
consumer.subscribe([topic])

print(f"Subscribed to topic: {topic}")

# Connexion à MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['air_quality_db']
collection = db['raw_data']

print("Connected to MongoDB")

try:
    while True:
        print("Polling for message...")
        msg = consumer.poll(1.0)

        if msg is None:
            print("No message received")
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('Reached end of partition {}'.format(msg.topic(), msg.partition()))
            else:
                print('Error: {}'.format(msg.error()))
        else:
            print("Received message")
            try:
                record = json.loads(msg.value().decode('utf-8'))
                collection.insert_one(record)
                print(f"Data stored in MongoDB: {record}")
            except json.JSONDecodeError:
                print(f"JSON decoding error for message: {msg.value()}")
except KeyboardInterrupt:
    print("Interruption detected, stopping consumer...")

finally:
    print("Closing consumer and MongoDB connection")
    consumer.close()
    client.close()