# TAT Profit/Loss Visualization Script

This script connects to a SQLite database containing trading data, fetches the data for today, and visualizes it using Plotly. The visualization includes premium sold and profit/loss (PnL) over time.

## Prerequisites

- Python 3.x
- SQLite database (`data.db3`)
- Required Python packages: `sqlite3`, `logging`, `datetime`, `timedelta`, `plotly.graph_objects`

## Installation

Ensure you have Python installed. You can download it from the [official Python website](https://www.python.org/).

1. Clone this repository to your local machine.

2. Install the required Python packages:

   ```bash
   pip install plotly
   pip install dash # Only needed if you want to run the live version`` 

Ensure you have the SQLite database (`data.db3`) in the same directory as the script.

## Usage

Run the script:


`python pnl.py` or `python live.py` 

The script will connect to the SQLite database and fetch trading data for today. It will then visualize the data using Plotly.

The visualization includes premium sold and PnL over time, along with annotations for the lowest, highest, and final PnL.

The graph will be displayed in your default web browser. Additionally, an HTML file (`{desired_day}_trading_data.html`) will be saved in the same directory containing the visualization.

## Script Explanation

-   The script connects to the SQLite database (`data.db3`) and fetches trading data for today.
-   It calculates the start datetime at 09:20 AM and the end datetime at 16:30 PM.
-   Data fetched includes `DailyLogID`, datetime, premium sold, and PnL.
-   The script visualizes the data using Plotly, including annotations for key PnL points.
-   The graph is displayed in the web browser and saved as an HTML file.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
