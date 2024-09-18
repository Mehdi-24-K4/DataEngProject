import requests
import json
from confluent_kafka import Producer
from constantes import API_URL, KAFKA_BROKER, KAFKA_TOPIC, MAX_LIMIT, API_KEY
import time

def fetch_air_quality_data(page=1):
    """
    Fetches air quality data from an API.
    Returns a list of results or an empty list if an error occurs.
    """
    params = {
        'limit': MAX_LIMIT,
        'page': page,
        'sort': 'desc',
        'has_geo': 'true'
    }

    headers = {
        'x-api-key': API_KEY, 
        "Cache-Control": "no-cache",
    }
    try:
        response = requests.get(API_URL, params=params, headers=headers)
        response.raise_for_status()
        data = response.json().get('results', [])
        print(f"Fetched {len(data)} records from page {page}.")  # Log the number of records and page number
        return data
    except requests.RequestException as e:
        print(f"Erreur lors de la récupération des données: {e}")
        return []


def getProducer():
    """
    This function initializes a Kafka producer.

    Returns:
        Producer: A Kafka producer object
    """
    # Define the bootstrap servers with the port
    bootstrap_servers = KAFKA_BROKER

    # Define the topic where the message will be published
    topic_name = KAFKA_TOPIC

    # Configure the producer
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'python-producer'
    }

    # Initialize the producer
    producer = Producer(conf)
    return producer

# Fonction de rapport de livraison
def delivery_report(err, msg):
    """
    Prints the delivery report of a Kafka message.

    Args:
        err (Optional[Exception]): The error that occurred during message delivery.
        msg (Message): The Kafka message.

    Returns:
        None
    """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def produce_air_quality_data():
    """
    Fetch air quality data and send it to Kafka.
    This function is designed to be used as a task in an Airflow DAG.
    """
    producer = getProducer()
    for page in (1,2):
        data = fetch_air_quality_data(page)
    
        if data:
            for item in data:
                try:
                    producer.produce(KAFKA_TOPIC, json.dumps(item).encode('utf-8'), callback=delivery_report)
                    producer.poll(1)  # Wait for the message to be delivered
                except Exception as e:
                    print(f'Failed to produce message: {e}')
            print(f"Données de la page {page} envoyées à Kafka")
        else:
            print(f"Données de la page {page} envoyées à Kafka")
    producer.flush()
    print("Envois des données terminé.")