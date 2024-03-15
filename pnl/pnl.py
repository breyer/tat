import sqlite3
import logging
from datetime import datetime, timedelta
import plotly.graph_objects as go
import os

# Define the directory name
directory = "pnl-log"

# Check if the directory exists
if not os.path.exists(directory):
    # If it does not exist, create it
    os.makedirs(directory)
    logging.info(f"Directory '{directory}' created.")

# Setup basic configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Connect to the SQLite database
conn = sqlite3.connect('data.db3')
logging.info("Connected to the database.")

# Get today's date
today = datetime.now().date()

# Desired day for visualization (changed to today's date)
desired_day = today.strftime('%Y-%m-%d')
#desired_day = (today - timedelta(days=1)).strftime('%Y-%m-%d')

# Calculate the start datetime at 09:00 AM
start_datetime = datetime.combine(today, datetime.min.time()) + timedelta(hours=9, minutes=20)

# Calculate the end datetime at 16:30 PM
end_datetime = start_datetime + timedelta(hours=7, minutes=40)

# Modified SQL query to fetch data for today, including SPX prices
query = """
SELECT
    DailyLogID,
    DATETIME(LogDate/10000000 - 62135596800, 'unixepoch') AS DT,
    PremiumSold,
    PL,
    SPX
FROM DailyLog
WHERE
    DATE(DT) = ?
    AND TIME(DT) BETWEEN TIME(?) AND TIME(?)
    AND SPX > 1.0  -- This line ensures only SPX prices higher than 1.0 are selected
ORDER BY DT ASC
"""

# Execute the query and fetch the data
cursor = conn.cursor()
cursor.execute(query, (desired_day, start_datetime.strftime('%H:%M'), end_datetime.strftime('%H:%M')))
logging.info("SQL query executed.")

# Fetch data
data = cursor.fetchall()

# Close the connection
conn.close()
logging.info("Database connection closed.")

# Check if data is empty
if not data:
    logging.error(f"No data found for today: {desired_day}")
    exit()

# Extract values for plotting, including SPX prices
dates = [datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S') for row in data]
premium_sold = [row[2] for row in data]
pl = [row[3] for row in data]
spx_prices = [row[4] for row in data]

# Identify the lowest, highest, and final PnL
min_pnl = min(pl)
max_pnl = max(pl)
final_pnl = pl[-1]
min_pnl_time = dates[pl.index(min_pnl)]
max_pnl_time = dates[pl.index(max_pnl)]
final_pnl_time = dates[-1]

# Find the high and low of SPX
spx_high = max(spx_prices)
spx_low = min(spx_prices)
spx_high_time = dates[spx_prices.index(spx_high)]
spx_low_time = dates[spx_prices.index(spx_low)]

# Define a function to dynamically adjust annotation positions to avoid overlaps
def adjust_annotation_position(data_points, current_point, y_values, current_value, index, default_ax=80, default_ay=-90):
    min_distance_x = timedelta(minutes=20)
    min_distance_y = max(y_values) * 0.05
    ax, ay = default_ax, default_ay
    for point in data_points:
        if abs((current_point - point).total_seconds()) < min_distance_x.total_seconds():
            ax *= -1
            break
    if index % 2 == 0:
        ay = -(current_value / max(y_values)) * 100
    else:
        ay = (current_value / max(y_values)) * 100
    if current_value > max(y_values) - min_distance_y:
        ay *= -1
    return ax, ay

# Corrected segment for annotations list initialization
annotations = []  # Initialize the annotations list at the beginning

# Define the function to create annotations with adjusted positions
def create_annotation(x, y, text, xref, yref, ax=80, ay=-90):
    # We no longer use `annotations` within `adjust_annotation_position` call
    ax, ay = adjust_annotation_position(dates, x, spx_prices if yref == 'y2' else pl, y, ax, ay)
    return go.layout.Annotation(
        x=x,
        y=y,
        xref=xref,
        yref=yref,
        text=f"<b>{text}</b>",
        showarrow=True,
        arrowhead=1,
        arrowsize=2,
        arrowwidth=1,
        arrowcolor='black',
        font=dict(size=16, color='black', family='Arial, sans-serif'),
        ax=ax,
        ay=ay
    )

# Create annotations for the graph, now including SPX high and low
annotations = [
    create_annotation(min_pnl_time, min_pnl, f"Lowest PnL: {min_pnl} ({min_pnl_time.strftime('%H:%M')})", 'x', 'y'),
    create_annotation(max_pnl_time, max_pnl, f"Highest PnL: {max_pnl} ({max_pnl_time.strftime('%H:%M')})", 'x', 'y'),
    create_annotation(final_pnl_time, final_pnl, f"Final PnL: {final_pnl} ({final_pnl_time.strftime('%H:%M')})", 'x', 'y'),
    # SPX high and low using the secondary y-axis
    create_annotation(spx_high_time, spx_high, f"SPX High: {spx_high} ({spx_high_time.strftime('%H:%M')})", 'x', 'y2'),
    create_annotation(spx_low_time, spx_low, f"SPX Low: {spx_low} ({spx_low_time.strftime('%H:%M')})", 'x', 'y2')
]

# Create traces for the graph, including a trace for SPX prices using the secondary y-axis
trace1 = go.Scatter(x=dates, y=premium_sold, mode='lines', name='Premium Sold', line=dict(color='blue'))
trace2 = go.Scatter(x=dates, y=pl, mode='lines', name='PnL', line=dict(color='orange'))
trace_spx = go.Scatter(
    x=dates, 
    y=spx_prices, 
    mode='lines', 
    name='SPX Price', 
    line=dict(color='green'),
    yaxis='y2'  # Assign to secondary y-axis
)

# Update the layout to include a secondary y-axis for the SPX
layout = go.Layout(
    title=f'PnL and SPX Data for {desired_day}',
    xaxis_title='Time',
    yaxis_title='PnL',
    annotations=annotations,
    xaxis=dict(type='date', range=[min(dates), max(dates)]),
    plot_bgcolor='rgb(230, 250, 255)', 
    paper_bgcolor='rgb(230, 250, 255)', 
    font=dict(color='black'),
    yaxis=dict(
        title='Premium Sold/PnL'
    ),
    yaxis2=dict(
        title='SPX Price',
        overlaying='y',  # This places the yaxis2 on top of the original y-axis
        side='right'  # This places the axis on the right side
    )
)

# Combine traces and layout in a figure
fig = go.Figure(data=[trace1, trace2, trace_spx], layout=layout)

# Set the marker line width for both traces
fig.update_traces(marker_line_width=2)

# Customize layout
fig.update_layout(
    title_font_size=24,
    xaxis_tickfont_size=14,
    yaxis_tickfont_size=14
)

# Show the graph
fig.show()

# Save the graph as an HTML file in the 'pnl-log' directory
file_name = os.path.join(directory, f'{desired_day}_trading_data.html')
fig.write_html(file_name)
logging.info(f"Graph saved as {file_name}.")
