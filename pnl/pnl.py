import sqlite3
import logging
from datetime import datetime, timedelta
import plotly.graph_objects as go

# Setup basic configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Connect to the SQLite database
conn = sqlite3.connect('data.db3')
logging.info("Connected to the database.")

# Get today's date
today = datetime.now().date()

# Desired day for visualization (changed to today's date)
desired_day = today.strftime('%Y-%m-%d')

# Calculate the start datetime at 09:00 AM
start_datetime = datetime.combine(today, datetime.min.time()) + timedelta(hours=9, minutes=20)

# Calculate the end datetime at 16:30 PM
end_datetime = start_datetime + timedelta(hours=7, minutes=30)

# SQL query to fetch data for today
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

# Execute the query and fetch the data
cursor = conn.cursor()
cursor.execute(query, (desired_day, start_datetime, end_datetime))
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

# Extract values for plotting
dates = [datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S') for row in data]
premium_sold = [row[2] for row in data]
pl = [row[3] for row in data]

# Identify the lowest, highest, and final PnL
min_pnl = min(pl)
max_pnl = max(pl)
final_pnl = pl[-1]
min_pnl_time = dates[pl.index(min_pnl)]
max_pnl_time = dates[pl.index(max_pnl)]
final_pnl_time = dates[-1]

# Define a function to create annotations with a standardized style
def create_annotation(x, y, text):
    return go.layout.Annotation(
        x=x, y=y, text=text, showarrow=True, arrowhead=1, arrowsize=2,
        arrowwidth=1, arrowcolor='black', font=dict(size=16, color='black'),
        ax=80, ay=-90  # these set the position of the text relative to the arrow
    )

# Create annotations for the graph
annotations = [
    create_annotation(min_pnl_time, min_pnl, f"Lowest PnL: {min_pnl} ({min_pnl_time.strftime('%H:%M')})"),
    create_annotation(max_pnl_time, max_pnl, f"Highest PnL: {max_pnl} ({max_pnl_time.strftime('%H:%M')})"),
    create_annotation(final_pnl_time, final_pnl, f"Final PnL: {final_pnl} ({final_pnl_time.strftime('%H:%M')})")
]

# Create traces for the graph
trace1 = go.Scatter(x=dates, y=premium_sold, mode='lines', name='Premium Sold', line=dict(color='blue'))
trace2 = go.Scatter(x=dates, y=pl, mode='lines', name='PnL', line=dict(color='orange'))

# Layout for the graph
layout = go.Layout(
    title=f'PnL Data for {desired_day}',
    xaxis_title='Time',
    yaxis_title='Value',
    annotations=annotations,
    xaxis=dict(type='date', range=[min(dates), max(dates)]),
    plot_bgcolor='rgb(230, 250, 255)', paper_bgcolor='rgb(230, 250, 255)', font=dict(color='black')
)

# Combine traces and layout in a figure
fig = go.Figure(data=[trace1, trace2], layout=layout)

# Set the marker line width for both traces
fig.update_traces(marker_line_width=2)

# Customize layout
fig.update_layout(
    title_font_size=24,
    xaxis_tickfont_size=14,
    yaxis_tickfont_size=14,
)

# Show the graph
fig.show()

# Save the graph as an HTML file (interactive graph)
file_name = f'{desired_day}_trading_data.html'
fig.write_html(file_name)
logging.info(f"Graph saved as {file_name}.")
