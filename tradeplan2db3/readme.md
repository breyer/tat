# Trade Plan Database Updater for TAT or Trade Automation Toolbox

## Overview
This repository contains a Python script designed to update trading parameters in the SQLite database of the **Trade Automation Toolbox** based on input from a CSV file. It focuses on modifying records in the `TradeTemplate` table and managing the `ScheduleMaster` table's active status. Additionally, the script ensures data integrity by creating a backup of the database before any modifications are made.

## Key Features

- **Automated Updating**: Updates trading parameters from CSV input seamlessly.
- **Backup Management**: Creates timestamped backups of the database to ensure data integrity.
- **Flexible Initialization**:
  - `--initialize`: Inserts missing `TradeConditions`, `TradeTemplates`, and `ScheduleMaster` entries without deleting existing data.
  - `--force-initialize`: Deletes existing entries and reinitializes the database with specified plans.
- **Schedule Management**: Manages the `IsActive` status in the `ScheduleMaster` table.
- **EMA Strategy Customization**: Tailored for EMA trading strategies with specific naming conventions.

## Setup Requirements

- **Python 3**
- **Required Python Libraries**:
  - `pandas` (for data handling)
  - `sqlite3` (for database interaction)
  - `shutil`, `os`, `argparse`, `sys` (standard libraries for file and system operations)

## Usage Instructions

1. **Ensure Prerequisites**:
   - Place `tradeplan.csv` and `data.db3` in the same directory as the script.

2. **Run the Script**:

   - **Standard Update**:
     
     To process the CSV file and update the database:
     
     ```bash
     python tradeplan2db3.py --qty 2
     ```
     
     **Optional**: To distribute contracts based on `PnL Rank`:
     
     ```bash
     python tradeplan2db3.py --qty 2 --distribution
     ```

   - **Initialize the Database**:
     
     - **Initialize Without Deleting Existing Data**:
       
       Inserts missing `TradeConditions`, `TradeTemplates`, and `ScheduleMaster` entries without affecting existing data:
       
       ```bash
       python tradeplan2db3.py --initialize
       ```
     
     - **Force Initialize (Delete Existing Data and Reinitialize)**:
       
       Deletes existing `TradeConditions`, `TradeTemplates`, and `ScheduleMaster` entries and reinitializes the database. You can specify the number of plans to initialize (default is 1):
       
       ```bash
       python tradeplan2db3.py --force-initialize 1
       ```

3. **Backup Management**:
   
   The script automatically creates a timestamped backup of the database before making any changes, stored in the `tradeplan-backup` directory as a ZIP file for efficiency.

## Command Line Options

- `--qty <integer>`: 
  - **Description**: Sets the quantity (`Qty`) for all entry times in the `TradeTemplate` table.
  - **Usage**: `--qty 2`

- `--distribution`: 
  - **Description**: Adjusts `Qty` based on `PnL Rank`. Adds 1 to top 1-3 ranks and subtracts 1 from ranks 8-10 if there are at least 10 entries.
  - **Usage**: `--distribution`

- `--initialize`: 
  - **Description**: Initializes the database by inserting missing `TradeConditions`, `TradeTemplates`, and `ScheduleMaster` entries without deleting existing data.
  - **Usage**: `--initialize`

- `--force-initialize [<integer>]`: 
  - **Description**: Forces the initialization of the database by deleting existing `TradeConditions`, `TradeTemplates`, and `ScheduleMaster` entries and reinitializes with the specified number of plans (default is 1 if no number is provided).
  - **Usage**: `--force-initialize 2`

## CSV File Structure

The CSV should include the following columns:

- `Hour:Minute`: Time in `HH:MM` format, e.g., `13:15`
- `Premium`: Premium value (float), e.g., `2.0`
- `Spread`: Spread value, can contain dashes which will be replaced by commas, e.g., `20-25-30`
- `Stop`: Stop value, typically a multiple with an `x`, e.g., `1.25x`
- `Strategy`: Strategy identifier, e.g., `EMA2040`
- `PnL Rank`: Rank based on Profit and Loss
- `Qty`: Quantity (integer) *(optional if using `--qty` parameter)*
- `Plan`: Plan identifier, e.g., `P1` *(optional; defaults to `P1` if missing)*

**Example**:

```csv
Hour:Minute,Premium,Spread,Stop,Strategy,PnL Rank,Qty,Plan
13:15,2.0,20-25-30,1.25x,EMA2040,1,2,P1
14:30,2.0,25-30-35,1.25x,EMA2040,2,2,P1
