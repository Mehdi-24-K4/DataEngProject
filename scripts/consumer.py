import json
from pymongo import MongoClient
from confluent_kafka import Consumer, KafkaException
from constantes import KAFKA_BROKER, KAFKA_TOPIC, MONGO_URI, MONGO_DB, MONGO_COLLECTION, KAFKA_DATA_LENGTH

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

    # Mettre à jour tous les documents qui n'ont pas le champ 'processed'
    collection.update_many(
        {"processed": {"$exists": False}},
        {"$set": {"processed": False}}
    )    
    # Return the MongoDB database
    return client[db_name]

def consume_and_store_data():
    consumer = get_consumer()
    db = connect_mongodb()

    try:
        messages_processed = 0
        # Consomme un certain nombre de messages (par exemple, 100 messages à la fois)
        for _ in range(KAFKA_DATA_LENGTH):  # ou ajuster le nombre selon vos besoins
            msg = consumer.poll(timeout=1000.0)  # Réduit le timeout pour une consommation rapide

            if msg is None:
                print('No new messages.')
                break  # Sortir de la boucle si aucun message

            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    print('End of partition reached {}/{}'.format(msg.topic(), msg.partition()))
                else:
                    raise KafkaException(msg.error())
            else:
                data = json.loads(msg.value().decode('utf-8'))

                # Ajoute la colonne 'processed' et la définit à False
                data['processed'] = False

                db[MONGO_COLLECTION].insert_one(data)
                messages_processed += 1
                print("Data inserted into the MongoDB database.")
        print(f"Total messages processed: {messages_processed}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        consumer.close()
        print('Consumer closed and resources released.')