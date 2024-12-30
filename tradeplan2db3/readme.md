# Trade Plan Database Updater for TAT (Trade Automation Toolbox)

## Overview

The `tradeplan2db3.py` script is designed to streamline and automate the process of updating your Trade Automation Toolbox (TAT) SQLite database with trading parameters sourced from a CSV file. It modifies `TradeTemplate` entries, manages the `ScheduleMaster` table for enabling or disabling strategies, and ensures data integrity through database backups.

In short, it allows you to:

- Import and update trading plans from a CSV file.
- Automatically create missing EMA-based `TradeCondition` entries.
- Initialize the database with custom `TradeTemplate` and `ScheduleMaster` entries.
- Adjust quantities based on Profit & Loss (PnL) rank if desired.
- Forcefully reinitialize the database (if needed), inserting or recreating all conditions, templates, and schedules.

**Important:** After importing and updating your database with this script, it is **recommended to restart TAT** to ensure all changes are properly recognized and applied.

## Changelog
**2024-12-30**
Added support for an optional OptionType column in tradeplan.csv.

When OptionType is present, the script updates only the template corresponding to "P" (PUT) or "C" (CALL) for each row.
When OptionType is missing, the script reverts to the legacy behavior of updating both PUT and CALL templates for every row.
This ensures backward compatibility with older CSV files that lack an OptionType column while allowing per-row control of PUT vs. CALL for newer CSV files.

- Refactoring of process_tradeplan:

Now detects whether OptionType exists and branches the update logic accordingly.
Introduced smaller helper methods (e.g., update_put_template, update_call_template, etc.) to keep code organized and easier to maintain.

- Improved error handling for OptionType:

If the column is present but contains invalid values (neither P nor C), the script logs an error and exits cleanly.
If the column is missing entirely, the script automatically falls back to updating both PUT and CALL spreads per row (the original approach).
These enhancements allow the script to handle both types of CSV file structures without requiring additional parameters or user intervention.

## Key Features

- **Automated Updates from CSV**:  
  Reads `tradeplan.csv` and updates the `TradeTemplate` table with parameters like `Premium`, `Spread`, and `StopMultiple`, as well as the `ScheduleMaster` table with corresponding conditions and activation status.
  
- **Backup Management**:  
  Automatically creates timestamped backups of your existing database before applying changes. These backups are compressed into `.zip` files and stored in a dedicated `tradeplan-backup` directory for easy rollback.

- **Flexible Initialization**:
  - `--initialize`: Inserts any missing `TradeConditions`, `TradeTemplates`, and `ScheduleMaster` entries without removing existing data.
  - `--force-initialize`: Removes existing entries and recreates them from scratch. Useful for resetting your environment or starting fresh.

- **Adjustable Quantities**:
  - Set a fixed `Qty` for all entries with `--qty`.
  - Optionally enable `--distribution` to modify quantities based on PnL ranks:
    - Adds 1 to the top 3 PnL ranks.
    - Subtracts 1 from ranks 8-10 (if at least 10 entries exist).

- **EMA Strategy Conditions**:  
  Automatically handles EMA-based conditions like `EMA5 > EMA20`, `EMA5 > EMA40`, and `EMA20 > EMA40`, as well as their inverse conditions. These conditions can be applied to both PUT and CALL spreads.

- **Schedule Management**:  
  Handles the `IsActive` status in `ScheduleMaster`. The initialization process can create schedules as inactive, allowing for careful review before activation. The main CSV-processing run reactivates schedules based on the updated parameters.

- **Recommended Restart of TAT After Import**:  
  Once the script completes and the database has been updated, a restart of TAT is recommended to ensure that all updates and changes are correctly loaded by TAT.

## Setup Requirements

- **Python 3**
- **Required Python Libraries**:
  - `pandas` (for data handling)
  - `sqlite3` (for database interaction; standard library)
  - `shutil`, `os`, `argparse`, `sys` (standard libraries)
  
Ensure these dependencies are installed before running the script.

## Preparing the Environment

1. **Files**:
   - Place `tradeplan.csv` and `data.db3` in the same directory as `tradeplan2db3.py`.
   - The script will read `tradeplan.csv` for instructions and modify `data.db3` accordingly.

2. **CSV File Structure**:
   The CSV file should contain the following columns:
   - `Hour:Minute` (e.g., `13:15`)
   - `Premium` (float, e.g., `2.0`)
   - `Spread` (string, can have dashes which are replaced by commas, e.g., `20-25-30`)
   - `Stop` (string/float, often a multiple with `x`, e.g., `1.25x`)
   - `Strategy` (string, e.g., `EMA2040`)
   - `PnL Rank` (integer rank)
   - `Qty` (integer, optional if `--qty` parameter is provided)
   - `Plan` (e.g., `P1`; optional, defaults to `P1` if missing)
   
   **Example**:
   ```csv
   Hour:Minute,Premium,Spread,Stop,Strategy,PnL Rank,Qty,Plan
   13:15,2.0,20-25-30,1.25x,EMA2040,1,2,P1
   14:30,2.0,25-30-35,1.25x,EMA2040,2,2,P1
