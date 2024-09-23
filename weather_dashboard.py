import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from cassandra.cluster import Cluster
import pandas as pd
from datetime import datetime, timedelta

# Cassandra configuration
CASSANDRA_HOST = "localhost"
KEYSPACE = "weather_keyspace"
TABLE = "weather_data"

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Indian Cities Weather Dashboard"),
    dcc.Dropdown(
        id='city-dropdown',
        options=[
            {'label': city, 'value': city} for city in 
            ['Mumbai', 'Delhi', 'Pune', 'Nagpur', 'Kolkata', 'Chennai', 'Bengaluru']
        ],
        value='Mumbai',
        multi=False
    ),
    html.H4("Temperature"),
    dcc.Graph(id='temperature-graph'),

    html.H4("Humidity"),
    dcc.Graph(id='humidity-graph'),

    html.H4("Wind Speed"),
    dcc.Graph(id='wind-speed-graph'),
    dcc.Interval(
        id='interval-component',
        interval=5*60*1000,  # update every 5 minutes
        n_intervals=0
    )
])

@app.callback(
    [Output('temperature-graph', 'figure'),
     Output('humidity-graph', 'figure'),
     Output('wind-speed-graph', 'figure')],
    [Input('city-dropdown', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_graphs(selected_city, n):
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)
    
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=24)
    
    query = f"SELECT * FROM {TABLE} WHERE city = %s AND timestamp >= %s AND timestamp <= %s ALLOW FILTERING"
    rows = session.execute(query, (selected_city, start_time, end_time))
    
    if not rows:
        return {}, {}, {}

    df = pd.DataFrame(rows)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    temp_trace = go.Scatter(
        x=df['timestamp'],
        y=df['temperature'],
        mode='lines+markers',
        name='Temperature'
    )
    humidity_trace = go.Scatter(
        x=df['timestamp'],
        y=df['humidity'],
        mode='lines+markers',
        name='Humidity'
    )
    wind_speed_trace = go.Scatter(
        x=df['timestamp'],
        y=df['wind_speed'],
        mode='lines+markers',
        name='Wind Speed'
    )
    
    temperature_fig = go.Figure(data=[temp_trace])
    humidity_fig = go.Figure(data=[humidity_trace])
    wind_speed_fig = go.Figure(data=[wind_speed_trace])
    
    return temperature_fig, humidity_fig, wind_speed_fig

if __name__ == '__main__':
    app.run_server(debug=True)