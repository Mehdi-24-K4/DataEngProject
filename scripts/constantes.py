"""
This file contains constants that are used in the program.
"""
API_URL = 'https://api.openaq.org/v2/latest'
"""
The URL of the OpenAQ API from which we will get real-time data
"""

API_KEY = 'YOUR_API_KEY_HERE'

# KAFKA_BROKER = 'localhost:29092' #For local run
KAFKA_BROKER = 'kafka:9092'
"""
The address of the Kafka broker that we will use to publish messages
"""
KAFKA_TOPIC = 'air_quality'
"""
The name of the Kafka topic to which we will publish messages
"""
MAX_LIMIT = 100
"""
The maximum number of messages that the client will fetch from Kafka
"""
# MONGO_URI = 'mongodb://localhost:27017/' #for local run
MONGO_URI = 'mongodb://mongodb:27017/'

"""
The URI of the MongoDB server where we will store the real-time data
"""
MONGO_DB = 'airquality'
"""
The name of the MongoDB database where we will store the real-time data
"""
MONGO_COLLECTION = 'raw_data'
"""
The name of the MongoDB collection where we will store the real-time data
"""
NUMBER_OF_PAGES = 2  # Nombre total de pages à consommer

KAFKA_DATA_LENGTH = 100*NUMBER_OF_PAGES
