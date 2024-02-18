
# Trade Plan Database Updater for TAT or Trade Automation Toolbox

## Overview
This repository contains a Python script designed to update trading parameters in a SQLite database based on input from a CSV file. It is particularly focused on modifying records in the `TradeTemplate` table and managing the `ScheduleMaster` table's active status. Additionally, the script ensures data integrity by creating a backup of the database before any modifications are made.

## Important Note
This script is specifically tailored for the EMA trading strategy and looks for `TradeTemplate` names formatted as "PUT SPREAD (HH:MM)" or "CALL SPREAD (HH:MM)". It is essential that each `TradeTemplate` entry in your database exactly matches this naming convention, as these names are critical identifiers in the trade setup and subsequent processing. Any deviation in naming could lead to mismatches or errors in processes that rely on these names for identification and execution of trade strategies. If your trade templates have different naming conventions, you will need to modify the script accordingly.

## Features
- Update trading parameters from a CSV file.
- Create a timestamped backup of the SQLite database.
- Automatically handles `IsActive` status in `ScheduleMaster` table.
- Tailored for EMA trading strategy with specific naming conventions.

## Requirements
- Python 3
- pandas
- sqlite3
- shutil, os (standard Python libraries)

## Usage
1. Place the `tradeplan.csv` file and the `data.db3` SQLite database in the same directory as the script.
2. Run the script using Python 3:
   ```bash
   python3 tradeplan2db3.py`` 

3.  The script will read the CSV file, update the database, and create a backup automatically.

## CSV File Format

The CSV file should contain the following columns:

-   `Hour:Minute`
-   `Premium`
-   `Spread`
-   `Stop`

## Database Tables

-   `TradeTemplate`: Contains the trade templates to be updated.
-   `ScheduleMaster`: Manages the active status of trade templates.

## Backup

The script creates a backup of the database file before making any changes. The backup is named with the current date and time and stored as a ZIP file.

## Script Functionality

-   **Import Modules**: Imports required modules for database and file operations.
-   **Define Paths**: Sets paths for the database and CSV file, ensuring the database path is absolute.
-   **Database Backup**: Generates a timestamped filename for backup, copies the database, and creates a ZIP archive.
-   **Database Connection**: Connects to the SQLite database.
-   **Read CSV Data**: Reads trade data from the CSV file.
-   **Update Database**: Iterates through CSV data to update `TradeTemplate` records and stores updated IDs.
-   **Manage ScheduleMaster Table**: Sets all `IsActive` fields to 0, then sets them to 1 for updated templates.
-   **Commit and Close**: Commits changes to the database and closes the connection.
-   **Cleanup**: Removes the non-zipped backup file.

## Contributions

Contributions to this project are welcome. Please ensure that you test your changes thoroughly before making a pull request.

## License

MIT License
