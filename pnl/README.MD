# TAT Profit/Loss Visualization Scripts

This directory contains two Python scripts for visualizing profit and loss (PnL) data from the Trade Automation Toolbox (TAT) SQLite database (`data.db3`).

![Example plot](https://github.com/breyer/tat/blob/main/plot-example.png?raw=true)

## Scripts

### 1. `pnl.py` - Static PnL Report

This script generates a static HTML report of the PnL for the current day. It connects to the database, fetches today's trading data, and creates an interactive Plotly graph. The graph includes annotations for the highest, lowest, and final PnL, as well as the high and low of the SPX.

### 2. `live.py` - Live PnL Dashboard

This script launches a live-updating web dashboard using Dash. The dashboard displays the PnL for the current day and automatically refreshes every 15 seconds to provide a real-time view of trading performance.

## Prerequisites

- Python 3.x
- A `data.db3` SQLite database file from the Trade Automation Toolbox in the same directory as the scripts.

## Installation

1.  **Clone the repository** to your local machine.
2.  **Install the required Python packages.**
    - For the static report (`pnl.py`):
      ```bash
      pip install plotly pandas
      ```
    - For the live dashboard (`live.py`), you also need Dash:
      ```bash
      pip install plotly pandas dash
      ```

## Usage

### Running the Static Report (`pnl.py`)

To generate a static PnL report for the current day, run the following command in your terminal:

```bash
python pnl.py
```

The script will:
- Create a `pnl-log` directory if it doesn't exist.
- Generate an interactive HTML file named `{current_date}_trading_data.html` inside the `pnl-log` directory.
- Open the generated HTML file in your default web browser.

### Running the Live Dashboard (`live.py`)

To launch the live PnL dashboard, run this command:

```bash
python live.py
```

The script will start a local web server and provide a URL (usually `http://127.0.0.1:8050/`). Open this URL in your web browser to view the live-updating dashboard.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
