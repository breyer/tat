
# Trade Plan Database Updater for TAT or Trade Automation Toolbox

## Overview
This repository contains a Python script designed to update trading parameters in the SQLite database of the "Trade Automation Toolbox" based on input from a CSV file. It is particularly focused on modifying records in the `TradeTemplate` table and managing the `ScheduleMaster` table's active status. Additionally, the script ensures data integrity by creating a backup of the database before any modifications are made.


## Important Note on Template Naming

The script is calibrated for use with trade templates named in a specific format: "PUT SPREAD (HH:MM)" or "CALL SPREAD (HH:MM)". It's imperative that your `TradeTemplate` entries in the database precisely follow this naming convention. These names serve as crucial identifiers for trade setups and are integral in the script's process of identifying and updating the correct records. If your templates follow a different naming convention, you will need to modify the script to align with your specific names.

### Identification of Trading Templates

The script employs a meticulous approach to identify trading templates, hinging on their adherence to a predefined naming convention:

1.  **Naming Convention**: Template names combine the trade type with a specific time, following the `[TRADE TYPE] ([TIME])` format, e.g., "PUT SPREAD (09:33)" or "CALL SPREAD (10:00)".
    
2.  **Trade Type**: This indicates the nature of the trade, such as "PUT SPREAD" or "CALL SPREAD", differentiating between put option spreads and call option spreads.
    
3.  **Time Component**: The time, enclosed in parentheses, is crucial for matching the trade strategy or timing, e.g., "09:33", "10:00".
    
4.  **Script's Matching Process**:
    
    -   Reads CSV data including time, premium, spread, and stop values.
    -   Constructs template names from trade type and CSV time data, then matches them with database records.
5.  **Database Update Process**:
    
    -   Identifies and updates the correct trading templates with values from the CSV file, ensuring all relevant templates are current.

## Key Features

-   Automated updating of trading parameters from CSV input.
-   Timestamped database backup for data integrity.
-   Management of `IsActive` status in the `ScheduleMaster` table.
-   Customized for EMA trading strategy and specific naming conventions.

## Setup Requirements

-   Python 3
-   pandas (for data handling)
-   sqlite3 (for database interaction)
-   shutil, os (standard libraries for file operations)

## Usage Instructions

1.  Ensure `tradeplan.csv` and `data.db3` are in the same directory as the script.
2.  Execute the script with Python 3:
    
    bashCopy code
    
    `python3 tradeplan2db3.py` 
    
3.  The script processes the CSV file, updates the database, and creates a backup autonomously.

## CSV File Structure

The CSV should include columns for:

-   `Hour:Minute`
-   `Premium`
-   `Spread`
-   `Stop`
-   `Strategy`

## Database Tables

-   `TradeTemplate`: Houses trade templates for updating.
-   `ScheduleMaster`: Oversees active status of templates.

## Backup Protocol

The script automatically generates a timestamped backup of the database pre-update, storing it as a ZIP file for efficiency.

## Detailed Script Functions

-   **Module Importation**: Loads necessary modules.
-   **Path Definitions**: Establishes paths for database and CSV.
-   **Database Backup**: Creates a backup file, copies the database, and archives it.
-   **Database Connection**: Initiates connection to the SQLite database.
-   **CSV Reading**: Imports trading data from CSV.
-   **Database Updating**: Modifies `TradeTemplate` records based on CSV data.
-   **ScheduleMaster Management**: Adjusts `IsActive` status based on updates.
-   **Commit and Closure**: Finalizes changes and disconnects from the database.
-   **Cleanup Process**: Eliminates the non-archived backup file.

## Tested TAT Versions
+ needs at least TAT 2.8.26.0

## Contributions

We welcome contributions! If you're interested in improving or adapting this tool, please feel free to submit pull requests. Ensure your changes are well-tested before submission.

## License

This project is released under the MIT License.
