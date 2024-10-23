from kafka import KafkaConsumer
import json
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import threading
import pandas as pd
from datetime import datetime, timedelta
import plotly.express as px
import sqlite3
from concurrent.futures import ThreadPoolExecutor
import numpy as np

# Database setup
def init_db():
    conn = sqlite3.connect('weather_history.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS weather_data
                 (city TEXT, temperature REAL, humidity REAL, wind_speed REAL,
                  pressure REAL, feels_like REAL, clouds INTEGER,
                  description TEXT, timestamp TEXT)''')
    conn.commit()
    conn.close()

class WeatherDataStorage:
    def __init__(self, db_path='weather_history.db'):
        self.db_path = db_path
        self.cities = ['London', 'Bengaluru', 'New York', 'Tokyo', 'Sydney', 'Paris', 'Chennai']
        self.cache = {city: {} for city in self.cities}
        self.cache_duration = timedelta(minutes=5)
        self.last_cache_update = datetime.now()
        init_db()

    def add_data(self, data):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''INSERT INTO weather_data VALUES 
                            (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                         (data['city'], data['temperature'], data['humidity'],
                          data['wind_speed'], data['pressure'], data['feels_like'],
                          data['clouds'], data['description'], data['timestamp']))
            conn.commit()
        self._update_cache(data['city'])

    def get_city_data(self, city, hours=24):
        if self._should_update_cache(city):
            self._update_cache(city)
        
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query('''
                SELECT * FROM weather_data 
                WHERE city = ? AND timestamp > datetime('now', ?)
                ORDER BY timestamp''',
                conn, params=(city, f'-{hours} hours'))
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df

    def get_current_data(self):
        current_data = {}
        for city in self.cities:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM weather_data 
                    WHERE city = ? 
                    ORDER BY timestamp DESC 
                    LIMIT 1''', (city,))
                result = cursor.fetchone()
                if result:
                    current_data[city] = {
                        'temperature': result[1],
                        'humidity': result[2],
                        'wind_speed': result[3],
                        'pressure': result[4],
                        'feels_like': result[5],
                        'clouds': result[6],
                        'description': result[7]
                    }
        return current_data

    def _should_update_cache(self, city):
        return (datetime.now() - self.last_cache_update) > self.cache_duration

    def _update_cache(self, city):
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query('''
                SELECT * FROM weather_data 
                WHERE city = ? 
                ORDER BY timestamp DESC 
                LIMIT 1000''', conn, params=(city,))
            self.cache[city] = df
            self.last_cache_update = datetime.now()

# Initialize components
consumer = KafkaConsumer(
    'weather-data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

data_storage = WeatherDataStorage()

# Create Dash app
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H3('Weather Monitoring Dashboard', className='text-center mb-4'),
    dcc.Tabs([
        dcc.Tab(label='Time Series', children=[
            html.Div([
                html.Div([
                    dcc.Dropdown(
                        id='city-dropdown',
                        options=[{'label': city, 'value': city} for city in data_storage.cities],
                        value='London',
                        className='mb-4'
                    ),
                    dcc.RadioItems(
                        id='time-range',
                        options=[
                            {'label': '24 Hours', 'value': 24},
                            {'label': '7 Days', 'value': 168},
                            {'label': '30 Days', 'value': 720}
                        ],
                        value=24,
                        className='mb-4'
                    )
                ], className='container'),
                dcc.Graph(id='live-graph'),
                html.Div(id='stats-container', className='mt-4')
            ])
        ]),
        dcc.Tab(label='Heat Maps', children=[
            dcc.Dropdown(
                id='heatmap-metric',
                options=[
                    {'label': 'Temperature', 'value': 'temperature'},
                    {'label': 'Humidity', 'value': 'humidity'},
                    {'label': 'Wind Speed', 'value': 'wind_speed'},
                    {'label': 'Pressure', 'value': 'pressure'}
                ],
                value='temperature',
                className='mb-4'
            ),
            dcc.Graph(id='heatmap-graph')
        ]),
        dcc.Tab(label='Statistics', children=[
            html.Div(id='statistics-container')
        ])
    ]),
    dcc.Interval(
        id='interval-component',
        interval=10000,
        n_intervals=0
    )
])

@app.callback(
    [Output('live-graph', 'figure'),
     Output('stats-container', 'children')],
    [Input('city-dropdown', 'value'),
     Input('time-range', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_time_series(city, hours, n):
    df = data_storage.get_city_data(city, hours)
    
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                        subplot_titles=('Temperature & Feels Like (°C)', 
                                      'Humidity (%) & Pressure (hPa)', 
                                      'Wind Speed (m/s) & Clouds (%)'))
    
    # Temperature and Feels Like
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['temperature'],
                            name='Temperature', line=dict(color='red')), row=1, col=1)
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['feels_like'],
                            name='Feels Like', line=dict(color='orange')), row=1, col=1)
    
    # Humidity and Pressure
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['humidity'],
                            name='Humidity', line=dict(color='blue')), row=2, col=1)
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['pressure'],
                            name='Pressure', line=dict(color='purple')), row=2, col=1)
    
    # Wind Speed and Clouds
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['wind_speed'],
                            name='Wind Speed', line=dict(color='green')), row=3, col=1)
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['clouds'],
                            name='Clouds', line=dict(color='gray')), row=3, col=1)

    fig.update_layout(height=800, showlegend=True)

    # Calculate statistics
    stats = html.Div([
        html.H4(f'Statistics for {city}'),
        html.Table([
            html.Tr([
                html.Td('Average Temperature:'),
                html.Td(f"{df['temperature'].mean():.1f}°C")
            ]),
            html.Tr([
                html.Td('Max Temperature:'),
                html.Td(f"{df['temperature'].max():.1f}°C")
            ]),
            html.Tr([
                html.Td('Min Temperature:'),
                html.Td(f"{df['temperature'].min():.1f}°C")
            ])
        ])
    ])

    return fig, stats

@app.callback(
    Output('heatmap-graph', 'figure'),
    [Input('heatmap-metric', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_heatmap(metric, n):
    current_data = data_storage.get_current_data()
    
    values = [data[metric] for city, data in current_data.items()]
    cities = list(current_data.keys())
    
    colorscale = 'RdYlBu_r' if metric == 'temperature' else 'Viridis'
    
    fig = go.Figure(data=go.Heatmap(
        z=[values],
        y=['Current'],
        x=cities,
        colorscale=colorscale,
        text=[[f'{val:.1f}' for val in values]],
        texttemplate='%{text}',
        textfont={"size": 12}
    ))
    
    title = f'Current {metric.title()} Across Cities'
    fig.update_layout(title=title, height=400)
    
    return fig

@app.callback(
    Output('statistics-container', 'children'),
    [Input('interval-component', 'n_intervals')]
)
def update_statistics(n):
    current_data = data_storage.get_current_data()
    
    stats_table = html.Table([
        html.Tr([html.Th('City')] + [html.Th(metric) for metric in 
                ['Temperature', 'Feels Like', 'Humidity', 'Wind Speed', 'Pressure', 'Clouds']])
    ] + [
        html.Tr([html.Td(city)] + [
            html.Td(f"{data[metric]:.1f}") for metric in 
            ['temperature', 'feels_like', 'humidity', 'wind_speed', 'pressure', 'clouds']
        ]) for city, data in current_data.items()
    ])
    
    return html.Div([
        html.H4('Current Weather Statistics'),
        stats_table
    ])

def kafka_consumer_thread():
    for message in consumer:
        data_storage.add_data(message.value)

if __name__ == '__main__':
    # Start Kafka consumer thread
    threading.Thread(target=kafka_consumer_thread, daemon=True).start()
    
    # Run Dash app
    app.run_server(debug=True)