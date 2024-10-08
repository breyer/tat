# Trading Analysis Tools (TAT)

Welcome to the GitHub repository for additional tools for the Trading Automation Toolbox (TAT). This repository contains several components that are essential for analyzing and updating trading data, as well as automating key tasks. Below is a brief overview of each.

## Components

### 1. [Trading Analysis Python Program (ipynb)](https://github.com/breyer/tat/tree/main/ipynb)

#### Overview

This is a Python program developed to analyze trading data from a CSV file. Utilizing libraries such as Pandas, Matplotlib, and Seaborn, it generates insightful heatmaps that reveal patterns in trading profits based on the time and day of trade entries.

#### Key Features

-   **Data Analysis**: Leverages Pandas for data manipulation and analysis.
-   **Heatmap Visualization**: Utilizes Matplotlib and Seaborn to create visual representations of trading data.
-   **Functionality**: Includes functions for filtering data, ranking trading days and times, and generating comprehensive heatmaps.

#### Workflow

-   Importing necessary libraries.
-   Defining functions for data handling and visualization.
-   Loading and preparing CSV data.
-   Analyzing data and generating HTML content with heatmaps.
-   Producing an HTML report containing visual and tabular representations of analyzed data.

### 2. [Trade Plan Database Updater (tradeplan2db3)](https://github.com/breyer/tat/tree/main/tradeplan2db3)

#### Overview

This component is a Python script designed to update trading parameters in the SQLite database for the "Trade Automation Toolbox." It focuses on modifying records in the `TradeTemplate` table and manages the `ScheduleMaster` table's active status.

#### Key Features

-   **Automated Database Updates**: Updates trading parameters based on CSV input.
-   **Backup Protocol**: Implements a timestamped backup system for data integrity.
-   **Customizable**: Tailored for specific naming conventions in trade templates.

#### Functionality

-   Identifies and updates trading templates based on CSV file input.
-   Manages `IsActive` status in the `ScheduleMaster` table.
-   Automatically generates a database backup before updating.

#### Usage

-   Requires Python 3, Pandas, sqlite3, shutil, and os.
-   The CSV file structure should include columns for time, premium, spread, and stop.
-   To use, place `tradeplan.csv` and `data.db3` in the script's directory and execute with Python 3.

### 3. [TAT Profit/Loss Visualization Script (pnl)](https://github.com/breyer/tat/tree/main/pnl)

#### Overview

This is a Python script that connects to a SQLite database containing trading data, fetches the data for the current day, and visualizes it using Plotly. The visualization includes metrics such as premium sold and profit/loss (PnL) over time.

![Example plot](https://github.com/breyer/tat/blob/main/plot-example.png?raw=true)

#### Key Features

-   **Real-Time Data Visualization**: Generates visual representations of trading data for immediate analysis.
-   **Comprehensive Metrics**: Showcases premium sold alongside profit and loss over the course of the day.
-   **Interactive Graphs**: Utilizes Plotly for dynamic and interactive charting capabilities.

#### Prerequisites

- Python 3.x
- SQLite database (`data.db3`)
- Required Python packages: `sqlite3`, `logging`, `datetime`, `timedelta`, `plotly.graph_objects`

### 4. [TAT Auto Login & Connect (tat-auto-login-connect)](https://github.com/breyer/tat/tree/main/tat-auto-login-connect)

#### Overview

This is a Python script designed to automate the login process for the Trade Automation Toolbox (TAT) and automatically connect it to your broker. By simulating key presses and mouse interactions, the script eliminates the need for manual login and broker connection each time you use TAT.

#### Key Features

-   **Automated Login**: Simulates the process of logging into TAT by entering email, password, and navigating through the interface.
-   **Broker Connection**: After login, the script waits 5 seconds, then connects TAT to the broker by pressing the necessary keys.
-   **Clipboard Management**: Utilizes the clipboard to handle special characters such as "@" in email addresses.
-   **Customizable Wait Times**: Includes configurable wait times to ensure smooth operation even on slower systems.

#### Requirements

##### Software
- **Python 3.x**: Ensure that Python is installed on your machine. You can download it from [python.org](https://www.python.org/).

##### Python Dependencies

To install the required dependencies, run the following command:

```bash
pip install pyautogui pygetwindow pyperclip
