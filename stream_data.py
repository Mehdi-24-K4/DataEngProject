import json
import time
from confluent_kafka import Producer
import requests

# Configuration Kafka
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'air_quality_data'

# Configuration API OpenAQ
API_URL = 'https://api.openaq.org/v2/latest'

def get_air_quality_data():
    """Récupère les données de qualité de l'air depuis l'API OpenAQ."""
    params = {
        'limit': 100,
        'page': 1,
        'sort': 'desc',
        'has_geo': 'true'
    }
    response = requests.get(API_URL, params=params)
    if response.status_code == 200:
        return response.json()['results']
    else:
        print(f"Erreur lors de la récupération des données: {response.status_code}")
        return None

def delivery_report(err, msg):
    """Callback appelé une fois pour chaque message produit pour indiquer le résultat final de la production."""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def create_kafka_producer():
    """Crée et retourne un producteur Kafka."""
    return Producer({'bootstrap.servers': KAFKA_BROKER})

def send_to_kafka(producer, data):
    """Envoie les données à Kafka."""
    for item in data:
        producer.produce(KAFKA_TOPIC, json.dumps(item).encode('utf-8'), callback=delivery_report)
    producer.flush()

def main():
    producer = create_kafka_producer()
    
    while True:
        print("Récupération des données de qualité de l'air...")
        air_quality_data = get_air_quality_data()
        
        if air_quality_data:
            print(f"Envoi de {len(air_quality_data)} enregistrements à Kafka...")
            send_to_kafka(producer, air_quality_data)
            print("Données envoyées avec succès.")
        else:
            print("Aucune donnée à envoyer.")
        
        # Attendre 5 minutes avant la prochaine requête
        time.sleep(300)

if __name__ == "__main__":
    main()