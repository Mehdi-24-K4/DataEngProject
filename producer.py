import requests
import json
from confluent_kafka import Producer
from constantes import API_URL, KAFKA_BROKER, KAFKA_TOPIC, MAX_LIMIT
import time

def fetch_air_quality_data():
    """
    Fetches air quality data from an API.
    Returns a list of results or an empty list if an error occurs.
    """
    params = {
        'limit': MAX_LIMIT,
        'page': 1,
        'sort': 'desc',
        'has_geo': 'true'
    }
    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        return response.json().get('results', [])
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

def main():
    producer = getProducer()
    try:
        while True:
            data = fetch_air_quality_data()
            if data:
                for item in data:
                    try:
                        producer.produce(KAFKA_TOPIC, json.dumps(item).encode('utf-8'), callback=delivery_report)
                        producer.poll(1)  # Attendre que le message soit livré
                    except Exception as e:
                        print(f'Failed to produce message: {e}')
                print("Données envoyées à Kafka")
            else:
                print("Pas de données sélectionnées.")
            print("Attente de 5 minutes avant le prochain appel.")
            time.sleep(300)  # Fetch data every 5 minutes
    except KeyboardInterrupt:
        producer.flush()
        print("Fin d'envois des données à Kafka")
        print("Interruption par l'utilisateur.")

if __name__ == "__main__":
    main()