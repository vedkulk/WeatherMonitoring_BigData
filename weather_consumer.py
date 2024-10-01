from kafka import KafkaConsumer
import json
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import threading

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'weather-data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Data storage
data_storage = {city: {'temps': [], 'hums': [], 'wind_speeds': [], 'times': []} for city in ['London', 'New York', 'Tokyo', 'Sydney', 'Paris']}

# Create Dash app
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H3('Real-Time Weather Data'),
    dcc.Dropdown(
        id='city-dropdown',
        options=[{'label': city, 'value': city} for city in data_storage.keys()],
        value='London'
    ),
    dcc.Graph(id='live-graph'),
    dcc.Interval(
        id='interval-component',
        interval=5*1000,  # in milliseconds
        n_intervals=0
    )
])

@app.callback(Output('live-graph', 'figure'),
              [Input('city-dropdown', 'value'),
               Input('interval-component', 'n_intervals')])
def update_graph(selected_city, n):
    city_data = data_storage[selected_city]
    
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                        subplot_titles=('Temperature (°C)', 'Humidity (%)', 'Wind Speed (m/s)'))

    line_colors = {
        'Temperature': 'red',
        'Humidity': 'blue',
        'Wind Speed': 'green'
    }

    fig.add_trace(go.Scatter(x=city_data['times'], y=city_data['temps'], mode='lines+markers', name='Temperature', 
                              line=dict(color=line_colors['Temperature'], width=2),
                              marker=dict(size=5, color=line_colors['Temperature'], symbol='circle')),
                  row=1, col=1)
    
    fig.add_trace(go.Scatter(x=city_data['times'], y=city_data['hums'], mode='lines+markers', name='Humidity', 
                              line=dict(color=line_colors['Humidity'], width=2),
                              marker=dict(size=5, color=line_colors['Humidity'], symbol='square')),
                  row=2, col=1)
    
    fig.add_trace(go.Scatter(x=city_data['times'], y=city_data['wind_speeds'], mode='lines+markers', name='Wind Speed', 
                              line=dict(color=line_colors['Wind Speed'], width=2),
                              marker=dict(size=5, color=line_colors['Wind Speed'], symbol='diamond')),
                  row=3, col=1)

    fig.update_layout(height=800, width=800, title=f'Real-Time Weather Data for {selected_city}',
                      showlegend=True, legend=dict(x=0.1, y=1.1, orientation='h'),
                      plot_bgcolor='rgba(230, 230, 230, 0.9)',
                      xaxis_title='Time (Seconds)',
                      yaxis_title='Values',
                      margin=dict(l=40, r=40, t=60, b=40))

    for i in range(1, 4):
        fig.update_yaxes(gridcolor='gray', row=i, col=1)
        fig.update_xaxes(showgrid=True, gridcolor='lightgray', row=i, col=1)

    return fig

def kafka_consumer_thread():
    for message in consumer:
        data = message.value
        city = data['city']
        if city in data_storage:
            city_data = data_storage[city]
            city_data['temps'].append(data['temperature'])
            city_data['hums'].append(data['humidity'])
            city_data['wind_speeds'].append(data['wind_speed'])
            city_data['times'].append(len(city_data['temps']))

if __name__ == '__main__':
    # Start Kafka consumer thread
    threading.Thread(target=kafka_consumer_thread, daemon=True).start()
    
    # Run Dash app
    app.run_server(debug=True)