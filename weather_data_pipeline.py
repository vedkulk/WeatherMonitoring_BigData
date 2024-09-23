import requests
from cassandra.cluster import Cluster
from dotenv import load_dotenv
import os
import time
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# OpenWeatherMap API configuration
API_KEY = os.getenv("OPENWEATHERMAP_API_KEY")
CITIES = [
    {"name": "Mumbai,in"},
    {"name": "Delhi,in"},
    {"name": "Pune,in"},
    {"name": "Nagpur,in"},
    {"name": "Kolkata,in"},
    {"name": "Chennai,in"},
    {"name": "Bengaluru,in"}
]
API_URL = "http://api.openweathermap.org/data/2.5/weather?q={city_name}&APPID={api_key}&units=metric"

# Cassandra configuration
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "weather_keyspace")
TABLE = os.getenv("CASSANDRA_TABLE", "weather_data")

def fetch_weather_data(city):
    try:
        url = API_URL.format(city_name=city['name'], api_key=API_KEY)
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return {
            "city": city['name'].split(',')[0],
            "temperature": data['main']['temp'],
            "humidity": data['main']['humidity'],
            "wind_speed": data['wind']['speed'],
            "timestamp": datetime.now()
        }
    except requests.RequestException as e:
        logging.error(f"Error fetching weather data for {city['name']}: {e}")
        return None

def setup_cassandra():
    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect()
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
            WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
        session.execute(f"""
            CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} (
                city text,
                temperature float,
                humidity float,
                wind_speed float,
                timestamp timestamp,
                PRIMARY KEY ((city), timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC)
        """)
        logging.info("Cassandra setup completed successfully")
        return session
    except Exception as e:
        logging.error(f"Error setting up Cassandra: {e}")
        return None

def insert_weather_data(session, data):
    try:
        query = f"""
            INSERT INTO {KEYSPACE}.{TABLE}
            (city, temperature, humidity, wind_speed, timestamp)
            VALUES (%s, %s, %s, %s, %s)
        """
        session.execute(query, (data['city'], data['temperature'], data['humidity'], data['wind_speed'], data['timestamp']))
        logging.info(f"Inserted data for {data['city']}")
    except Exception as e:
        logging.error(f"Error inserting data into Cassandra: {e}")

def main():
    session = setup_cassandra()
    if not session:
        logging.error("Failed to set up Cassandra. Exiting.")
        return

    while True:
        for city in CITIES:
            weather_data = fetch_weather_data(city)
            if weather_data:
                insert_weather_data(session, weather_data)
        logging.info("Sleeping for 5 minutes before next data collection")
        time.sleep(300)  # Sleep for 5 minutes

if __name__ == "__main__":
    main()