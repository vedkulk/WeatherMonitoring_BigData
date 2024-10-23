from kafka import KafkaProducer
import requests
import json
import time
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# OpenWeatherMap API configuration
API_KEY = os.getenv("OPENWEATHERMAP_API_KEY")
CITIES = ['London', 'Bengaluru', 'New York', 'Tokyo', 'Sydney', 'Paris', 'Chennai']
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
        
        # Enhanced weather data with more fields and timestamp
        weather = {
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'wind_speed': data['wind']['speed'],
            'pressure': data['main']['pressure'],
            'description': data['weather'][0]['description'],
            'city': city,
            'timestamp': datetime.now().isoformat(),
            'feels_like': data['main']['feels_like'],
            'clouds': data['clouds']['all']
        }
        return weather
    except Exception as e:
        print(f"Error fetching data for {city}: {e}")
        return None

def main():
    while True:
        for city in CITIES:
            weather_data = get_weather_data(city)
            if weather_data:
                print(f"Sending data: {weather_data}")
                producer.send('weather-data', value=weather_data)
        time.sleep(10)  # Fetch data every 10 seconds

if __name__ == "__main__":
    main()