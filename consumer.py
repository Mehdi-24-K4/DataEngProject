import json
from pymongo import MongoClient
from confluent_kafka import Consumer, KafkaException
from constantes import KAFKA_BROKER, KAFKA_TOPIC, MONGO_URI, MONGO_DB, MONGO_COLLECTION

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

    # Return the MongoDB database
    return client[db_name]

def main():
    consumer = get_consumer()
    db = connect_mongodb()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    print('End of partition reached {}/{}'.format(msg.topic(), msg.partition()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Convert the message to a Python dictionary
                data = json.loads(msg.value().decode('utf-8'))

                # Insert the data into the MongoDB database
                db[MONGO_COLLECTION].insert_one(data)

                # Print the message in the terminal
                print("Data inserted into the MongoDB database.")
                print("Topic Name={}, Message={}".format(msg.topic(), msg.value().decode('utf-8')))
    except KeyboardInterrupt:
        print('Aborted by user')
    finally:
        # Close the consumer to release resources
        consumer.close()
        # Close the MongoDB client
        db.client.close()

if __name__ == "__main__":
    main()
