"""
This file contains constants that are used in the program.
"""
API_URL = 'https://api.openaq.org/v2/latest'
"""
The URL of the OpenAQ API from which we will get real-time data
"""

API_KEY = '625766118f3124ca6140bd0d9f7862bcd2a802ad05ebc8a239194ca8108e2308'

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

KAFKA_DATA_LENGTH = 100


JAR_PATH = "C:/Users/amine/OneDrive/Bureau/ProjetDataEngMehdi/pilotes/postgresql-42.7.3.jar"