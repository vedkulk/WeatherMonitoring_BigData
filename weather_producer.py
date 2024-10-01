from kafka import KafkaProducer
import requests
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()

# OpenWeatherMap API configuration
API_KEY = os.getenv("OPENWEATHERMAP_API_KEY")
CITIES = ['London', 'New York', 'Tokyo', 'Sydney', 'Paris']
BASE_URL = 'http://api.openweathermap.org/data/2.5/weather'

# Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092', 
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def get_weather_data(city):
    try:
        params = {
            'q': city,
            'appid': API_KEY,
            'units': 'metric'
        }
        response = requests.get(BASE_URL, params=params)
        data = response.json()
        weather = {
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'wind_speed': data['wind']['speed'],
            'city': city
        }
        return weather
    except Exception as e:
        print(f"Error fetching data for {city}: {e}")
        return None

while True:
    for city in CITIES:
        weather_data = get_weather_data(city)
        if weather_data:
            print(f"Sending data: {weather_data}")
            producer.send('weather-data', value=weather_data)
    time.sleep(60)  # Fetch data every minute