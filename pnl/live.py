import sqlite3
import logging
from datetime import datetime, timedelta
import plotly.graph_objects as go
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import argparse

# Setup basic configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_data_for_date(db_path, date, start_time, end_time):
    """Gets trading data for a specific date from the database.

    Args:
        db_path (str): The path to the SQLite database.
        date (str): The date to get data for, in 'YYYY-MM-DD' format.
        start_time (str): The start time to get data for, in 'HH:MM' format.
        end_time (str): The end time to get data for, in 'HH:MM' format.

    Returns:
        list: A list of tuples containing the trading data.
    """
    with sqlite3.connect(db_path) as conn:
        query = """
        SELECT
            DailyLogID,
            DATETIME(LogDate/10000000 - 62135596800, 'unixepoch') AS DT,
            PremiumSold,
            PL
        FROM DailyLog
        WHERE
            DATE(DT) = ?
            AND TIME(DT) BETWEEN TIME(?) AND TIME(?)
        ORDER BY DT ASC
        """
        cursor = conn.cursor()
        cursor.execute(query, (date, start_time, end_time))
        return cursor.fetchall()

def create_figure(data, date):
    """Creates a Plotly figure from trading data.

    Args:
        data (list): A list of tuples containing the trading data.
        date (str): The date of the trading data, in 'YYYY-MM-DD' format.

    Returns:
        go.Figure: A Plotly figure object.
    """
    if not data:
        logging.error(f"No data found for today: {date}")
        return go.Figure()

    dates, premium_sold, pl = zip(*[(datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S'), row[2], row[3]) for row in data])
    fig = go.Figure(data=[
        go.Scatter(x=dates, y=premium_sold, mode='lines', name='Premium Sold', line=dict(color='blue')),
        go.Scatter(x=dates, y=pl, mode='lines', name='PnL', line=dict(color='orange'))
    ])
    fig.update_layout(
        title=f'PnL Data for {date}',
        xaxis_title='Time',
        yaxis_title='Value',
        xaxis=dict(type='date', range=[min(dates), max(dates)]),
        plot_bgcolor='rgb(230, 250, 255)',
        paper_bgcolor='rgb(230, 250, 255)',
        font=dict(color='black'),
        title_font_size=24,
        xaxis_tickfont_size=14,
        yaxis_tickfont_size=14
    )
    return fig

# --- Dash App Setup ---
# Initialize the Dash application.
app = dash.Dash(__name__)

# Define the layout of the Dash application.
# It consists of a graph that is updated by a timer.
app.layout = html.Div([
    dcc.Graph(id='live-update-graph'),
    dcc.Interval(
        id='interval-component',
        interval=15*1000,  # milliseconds
        n_intervals=0
    )
])

@app.callback(Output('live-update-graph', 'figure'),
              [Input('interval-component', 'n_intervals')],
              [dash.dependencies.State('live-update-graph', 'figure')])
def update_graph_live(n, existing_figure):
    """Updates the graph with live data.

    Args:
        n (int): The number of intervals that have passed.
        existing_figure (go.Figure): The existing Plotly figure.

    Returns:
        go.Figure: The updated Plotly figure.
    """
    # Modify these values or use argparse to make them command-line arguments
    db_path = 'data.db3'
    today_str = datetime.now().date().strftime('%Y-%m-%d')
    start_time, end_time = "09:20", "16:30"
    data = get_data_for_date(db_path, today_str, start_time, end_time)
    fig = create_figure(data, today_str)
    return fig

if __name__ == '__main__':
    # Main execution block
    app.run_server(debug=True)
