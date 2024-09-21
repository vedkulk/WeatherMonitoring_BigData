import requests
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from cassandra.cluster import Cluster
import json
from dotenv import load_dotenv
import os
import time

# Load environment variables
load_dotenv()

# OpenWeatherMap API configuration
API_KEY = os.getenv("OPENWEATHERMAP_API_KEY")
CITIES = [
    {"id": "1275339", "name": "Mumbai"},
    {"id": "1273294", "name": "Delhi"},
    {"id": "1259229", "name": "Pune"},
    {"id": "1262180", "name": "Nagpur"},
    {"id": "1275004", "name": "Kolkata"},
    {"id": "1264527", "name": "Chennai"},
    {"id": "1277333", "name": "Bengaluru"}
]
API_URL = "http://api.openweathermap.org/data/2.5/weather?id={city_id}&appid={api_key}&units=metric"

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "weather_data"

# Cassandra configuration
CASSANDRA_HOST = "localhost"
KEYSPACE = "weather_keyspace"
TABLE = "weather_data"

def fetch_weather_data(city):
    url = API_URL.format(city_id=city['id'], api_key=API_KEY)
    response = requests.get(url)
    data = response.json()
    return {
        "city": city['name'],
        "temperature": data['main']['temp'],
        "humidity": data['main']['humidity'],
        "wind_speed": data['wind']['speed']
    }

def send_to_kafka(data):
    producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer.send(KAFKA_TOPIC, data)
    producer.close()

def process_with_spark():
    spark = SparkSession.builder.appName("WeatherDataProcessing").getOrCreate()
    
    # Define schema for the incoming JSON data
    schema = StructType([
        StructField("city", StringType()),
        StructField("temperature", FloatType()),
        StructField("humidity", FloatType()),
        StructField("wind_speed", FloatType())
    ])

    # Create streaming DataFrame from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    # Parse JSON data
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Process and write to Cassandra
    query = parsed_df \
        .writeStream \
        .foreachBatch(lambda batch_df, batch_id: batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table=TABLE, keyspace=KEYSPACE) \
            .save()) \
        .start()

    query.awaitTermination()

def main():
    # Set up Cassandra keyspace and table
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()
    session.execute(f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
    session.execute(f"CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} (city text, temperature float, humidity float, wind_speed float, timestamp timestamp, PRIMARY KEY ((city), timestamp)) WITH CLUSTERING ORDER BY (timestamp DESC)")
    session.shutdown()

    # Start Spark processing in a separate thread
    import threading
    spark_thread = threading.Thread(target=process_with_spark)
    spark_thread.start()

    # Fetch data and send to Kafka continuously
    while True:
        for city in CITIES:
            weather_data = fetch_weather_data(city)
            send_to_kafka(weather_data)
        time.sleep(300)  # Wait for 5 minutes before the next round of data fetching

if __name__ == "__main__":
    main()