import json
from pymongo import MongoClient, InsertOne
from confluent_kafka import Consumer, KafkaException
from constantes import KAFKA_BROKER, KAFKA_TOPIC, MONGO_URI, MONGO_DB, MONGO_COLLECTION, KAFKA_DATA_LENGTH
import copy

def get_consumer():
    """
    Initializes and returns a Kafka consumer.

    This function sets up a Kafka consumer by defining the bootstrap servers with the port,
    the topic from which the message will be received, and the consumer configuration.
    The consumer is then subscribed to the specified topic.

    Returns:
        Consumer: A Kafka consumer object.
    """
    # Define the bootstrap servers with the port
    bootstrap_servers = KAFKA_BROKER

    # Define the name of the topic from which the message will be received
    topic_name = KAFKA_TOPIC

    # Define the consumer configuration
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'python-consumer',
        'auto.offset.reset': 'earliest'
    }

    # Initialize the consumer
    consumer = Consumer(conf)

    # Subscribe to the topic
    consumer.subscribe([topic_name])
    return consumer

def connect_mongodb():
    """
    Initializes and returns a MongoDB client.

    This function sets up a MongoDB client by defining the URI of the MongoDB server
    and the database and collection where the real-time data will be stored.

    Returns:
        MongoClient: A MongoDB client object.
    """
    # Define the URI of the MongoDB server
    uri = MONGO_URI

    # Define the name of the database
    db_name = MONGO_DB

    # Initialize the MongoDB client
    client = MongoClient(uri)

    db = client[db_name]
    collection = db[MONGO_COLLECTION]

    # # Mettre à jour tous les documents qui n'ont pas le champ 'processed'
    collection.update_many(
        {"processed": {"$exists": False}},
        {"$set": {"processed": False}}
    )    
    # Return the MongoDB database
    return client[db_name]


def normalize_data(data):
    """ Normalise les données pour une comparaison cohérente """
    # Faire une copie des données pour ne pas altérer les originaux
    normalized_data = copy.deepcopy(data)
    
    # Trier les mesures par le paramètre 'parameter' pour garantir une comparaison ordonnée
    if 'measurements' in normalized_data:
        normalized_data['measurements'] = sorted(normalized_data['measurements'], key=lambda x: x['parameter'])
    
    return normalized_data



def consume_and_store_data():
    consumer = get_consumer()
    db = connect_mongodb()
    collection = db[MONGO_COLLECTION]

    try:
        messages_processed = 0
        bulk_operations = []

        while messages_processed < KAFKA_DATA_LENGTH:
            msg = consumer.poll(timeout=1000.0)
            if msg is None:
                continue  # Continuez à attendre des messages
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    print('End of partition reached {}/{}'.format(msg.topic(), msg.partition()))
                else:
                    raise KafkaException(msg.error())
            else:
                data = json.loads(msg.value().decode('utf-8'))
                data['processed'] = False
                # Créez un filtre qui correspond exactement à tous les champs du message
                filter_doc = {
                    "location": data["location"],
                    "city": data["city"],
                    "country": data["country"],
                    "coordinates": data["coordinates"],
                    "measurements": {
                        "$all": [
                            {
                                "$elemMatch": {
                                    "parameter": m["parameter"],
                                    "value": m["value"],
                                    "lastUpdated": m["lastUpdated"],
                                    "unit": m["unit"]
                                }
                            } for m in data["measurements"]
                        ]
                    }
                }

                # Vérifiez si un document exactement identique existe déjà
                existing_doc = collection.find_one(filter_doc)

                if existing_doc is None:
                    # Le document n'existe pas, préparez l'opération d'insertion
                    operation = InsertOne(data)
                    bulk_operations.append(operation)
                    print(f"New document prepared for insertion: {data}")
                else:
                    print(f"Exact document already exists for location: {data}, skipping insertion.")

                messages_processed += 1

                # Exécutez les opérations en bloc toutes les 100 messages ou à la fin
                if len(bulk_operations) >= 100 or messages_processed == KAFKA_DATA_LENGTH:
                    if bulk_operations:
                        result = collection.bulk_write(bulk_operations)
                        print(f"Inserted: {result.inserted_count}")
                    bulk_operations = []

        print(f"Total messages processed: {messages_processed}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        consumer.close()
        print('Consumer closed and resources released.')