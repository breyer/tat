# Trade Plan to Database Updater

## Overview

The `tradeplan2db3.py` script is a powerful command-line tool for managing the trading strategies in your Trade Automation Toolbox (TAT) SQLite database (`data.db3`). It reads a simple CSV file (`tradeplan.csv`) and uses it to create, update, and manage your `TradeTemplate` and `ScheduleMaster` entries, allowing you to automate your trading setup efficiently.

## Key Features

- **Automated Database Updates**: Populates the database with trade templates and schedules directly from a CSV file.
- **Timestamped Backups**: Automatically creates a compressed `.zip` backup of your database before any changes are made.
- **Flexible Initialization**:
  - **`--initialize`**: Safely adds any missing conditions, templates, or schedules without deleting existing data. Perfect for setting up a new plan.
  - **`--force-initialize`**: Wipes all existing trade templates and schedules to create a clean setup from scratch.
- **Quantity and Distribution Control**:
  - **`--qty`**: Set a uniform quantity for all trades.
  - **`--distribution`**: Automatically adjust trade quantities based on a "PnL Rank" column in your CSV.
- **Dynamic EMA Conditions**: Automatically creates and manages the required EMA-based `TradeCondition` entries in the database.
- **Logging**: Keeps a detailed record of all operations in `tradeplan_updates.log`.

**Important**: After running the script, it is recommended to **restart TAT** to ensure that all changes are loaded correctly.

## Requirements

- Python 3.x
- `pandas` library. You can install it via pip:
  ```bash
  pip install pandas
  ```

## Setup and Usage

### 1. Prepare Your Files

Place the following files in the same directory as the `tradeplan2db3.py` script:
- `data.db3`: Your Trade Automation Toolbox SQLite database.
- `tradeplan.csv`: Your trade plan file.

### 2. Structure Your `tradeplan.csv`

Your CSV file is the heart of the process. It should contain the following columns:

| Column         | Description                                                 | Example        |
|----------------|-------------------------------------------------------------|----------------|
| `Hour:Minute`  | The entry time for the trade.                               | `13:15`        |
| `Premium`      | The target premium (maps to `TargetMax` or `TargetMaxCall`). | `2.0`          |
| `Spread`       | The desired long widths, separated by commas or dashes.     | `20,25,30`     |
| `Stop`         | The stop-loss multiple.                                     | `1.25`         |
| `Strategy`     | The EMA strategy (`EMA520`, `EMA540`, `EMA2040`).            | `EMA2040`      |
| `Plan`         | The plan identifier (e.g., "P1", "P2"). Defaults to `P1`.   | `P1`           |
| `Qty`          | The quantity for the trade.                                 | `2`            |
| `profittarget` | The profit target as a percentage (e.g., 50 for 50%).      | `50`           |
| `OptionType`   | `P` for PUT, `C` for CALL. If blank, applies to both.       | `P`            |
| `PnL Rank`     | An integer rank, used for the `--distribution` flag.        | `1`            |

**Example `tradeplan.csv`:**
```csv
Hour:Minute,Premium,Spread,Stop,Strategy,Plan,Qty,profittarget,OptionType,PnL Rank
13:15,2.0,20-25-30,1.25,EMA2040,P1,2,50,P,1
14:30,2.0,25-30-35,1.25,EMA2040,P1,2,50,C,2
```

### 3. Run the Script

You can run the script in several modes:

#### Standard Update (Most Common)
This will deactivate all existing schedules and then update and reactivate them based on your `tradeplan.csv`.

```bash
python tradeplan2db3.py
```

#### Initialize a New Plan
Use this when you have added a new plan (e.g., "P2") to your CSV and need to create the corresponding templates and schedules in the database.

```bash
python tradeplan2db3.py --initialize
```
The script will prompt you to enter your account ID(s).

#### Force a Clean Re-initialization
This will **delete all** existing trade templates and schedules and rebuild them from scratch based on a specified number of plans.

```bash
python tradeplan2db3.py --force-initialize
```
The script will prompt you for the number of plans to create (e.g., `1` for just "P1") and your account ID(s).

#### Set a Global Quantity
Override the `Qty` column in the CSV and set a single quantity for all trades.

```bash
python tradeplan2db3.py --qty 5
```

## License

This project is licensed under the MIT License. See the LICENSE file for details.
