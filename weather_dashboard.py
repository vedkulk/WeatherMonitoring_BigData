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
    dcc.Graph(id='temperature-graph'),
    dcc.Graph(id='humidity-graph'),
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
    # Connect to Cassandra and fetch data
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)
    
    # Fetch data for the last 24 hours
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=24)
    
    query = f"SELECT * FROM {TABLE} WHERE city = %s AND timestamp >= %s AND timestamp <= %s ALLOW FILTERING"
    rows = session.execute(query, (selected_city, start_time, end_time))
    
    df = pd.DataFrame(rows)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    
    # Create traces
    temp_trace = go.Scatter(
        x=df['timestamp'],
        y=df['temperature'],
        name='Temperature',
        mode='lines+markers'
    )
    
    humidity_trace = go.Scatter(
        x=df['timestamp'],
        y=df['humidity'],
        name='Humidity',
        mode='lines+markers'
    )
    
    wind_trace = go.Scatter(
        x=df['timestamp'],
        y=df['wind_speed'],
        name='Wind Speed',
        mode='lines+markers'
    )

    # Create layouts
    temp_layout = go.Layout(
        title=f'Temperature in {selected_city}',
        xaxis=dict(title='Time'),
        yaxis=dict(title='Temperature (°C)')
    )
    
    humidity_layout = go.Layout(
        title=f'Humidity in {selected_city}',
        xaxis=dict(title='Time'),
        yaxis=dict(title='Humidity (%)')
    )
    
    wind_layout = go.Layout(
        title=f'Wind Speed in {selected_city}',
        xaxis=dict(title='Time'),
        yaxis=dict(title='Wind Speed (m/s)')
    )

    return (
        {'data': [temp_trace], 'layout': temp_layout},
        {'data': [humidity_trace], 'layout': humidity_layout},
        {'data': [wind_trace], 'layout': wind_layout}
    )

if __name__ == '__main__':
    app.run_server(debug=True)