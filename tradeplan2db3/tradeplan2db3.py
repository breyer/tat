import pandas as pd
import sqlite3
from datetime import datetime
import shutil
import os

# Path to your database file
db_path = 'data.db3'

# Ensuring the path is absolute
db_path = os.path.abspath(db_path)

# Path to your CSV file
csv_path = 'tradeplan.csv'

# Create a backup of the database
current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
backup_filename = f'data_backup_{current_datetime}'
backup_filepath = backup_filename + '.db3'

try:
    # Copy the original database file to the backup path
    shutil.copy(db_path, backup_filepath)

    # Create a ZIP archive of the backup
    shutil.make_archive(backup_filename, 'zip', '.', backup_filepath)
except FileNotFoundError:
    print("Error: Database file not found. Please check the file path.")
    exit(1)
except shutil.Error as e:
    print(f"Error occurred while creating backup: {str(e)}")
    exit(1)

try:
    # Establish a connection to the SQLite database
    conn = sqlite3.connect(db_path)
except sqlite3.Error as e:
    print(f"Error occurred while connecting to the database: {str(e)}")
    exit(1)

try:
    # Read data from the CSV file
    data = pd.read_csv(csv_path, delimiter=',', quotechar='"')
except FileNotFoundError:
    print("Error: CSV file not found. Please check the file path.")
    conn.close()
    exit(1)
except pd.errors.EmptyDataError:
    print("Error: CSV file is empty.")
    conn.close()
    exit(1)

# List to store the updated TradeTemplateIDs
updated_ids = []

# Making changes in the database
for index, row in data.iterrows():
    hour_minute = row['Hour:Minute']
    premium = row['Premium']
    spread = row['Spread']
    stop = row['Stop']
    strategy = row['Strategy']
    quantity = row.get('Quantity', None)

    try:
        # Update for PUT SPREAD
        cursor = conn.execute(f"""
            UPDATE TradeTemplate
            SET TargetMax = {premium}, StopMultiple = '{stop}', LongWidth = {spread}
            WHERE Name = 'PUT SPREAD ({hour_minute})'
            RETURNING TradeTemplateID
        """)
        put_ids = [id[0] for id in cursor.fetchall()]
        updated_ids.extend(put_ids)

        # Update for CALL SPREAD
        cursor = conn.execute(f"""
            UPDATE TradeTemplate
            SET TargetMaxCall = {premium}, StopMultiple = '{stop}', LongWidth = {spread}
            WHERE Name = 'CALL SPREAD ({hour_minute})'
            RETURNING TradeTemplateID
        """)
        call_ids = [id[0] for id in cursor.fetchall()]
        updated_ids.extend(call_ids)
    except sqlite3.Error as e:
        print(f"Error occurred while updating TradeTemplate: {str(e)}")
        conn.close()
        exit(1)

    try:
        # Update Conditions, Strategy, and QtyOverride in ScheduleMaster
        for trade_template_id in put_ids:
            if strategy == 'EMA540':
                conn.execute(f"UPDATE ScheduleMaster SET Condition = 'EMA5 > EMA40', Strategy = '{strategy}' WHERE TradeTemplateID = {trade_template_id}")
            elif strategy == 'EMA520':
                conn.execute(f"UPDATE ScheduleMaster SET Condition = 'EMA5 > EMA20', Strategy = '{strategy}' WHERE TradeTemplateID = {trade_template_id}")
            elif strategy == 'EMA2040':
                conn.execute(f"UPDATE ScheduleMaster SET Condition = 'EMA20 > EMA40', Strategy = '{strategy}' WHERE TradeTemplateID = {trade_template_id}")
            if quantity is not None and not pd.isna(quantity):
                conn.execute(f"UPDATE ScheduleMaster SET QtyOverride = {quantity} WHERE TradeTemplateID = {trade_template_id}")
        
        for trade_template_id in call_ids:
            if strategy == 'EMA540':
                conn.execute(f"UPDATE ScheduleMaster SET Condition = 'EMA5 < EMA40', Strategy = '{strategy}' WHERE TradeTemplateID = {trade_template_id}")
            elif strategy == 'EMA520':
                conn.execute(f"UPDATE ScheduleMaster SET Condition = 'EMA5 < EMA20', Strategy = '{strategy}' WHERE TradeTemplateID = {trade_template_id}")
            elif strategy == 'EMA2040':
                conn.execute(f"UPDATE ScheduleMaster SET Condition = 'EMA20 < EMA40', Strategy = '{strategy}' WHERE TradeTemplateID = {trade_template_id}")
            if quantity is not None and not pd.isna(quantity):
                conn.execute(f"UPDATE ScheduleMaster SET QtyOverride = {quantity} WHERE TradeTemplateID = {trade_template_id}")
    except sqlite3.Error as e:
        print(f"Error occurred while updating ScheduleMaster: {str(e)}")
        conn.close()
        exit(1)

try:
    # Set IsActive to 0 in the ScheduleMaster table
    conn.execute("UPDATE ScheduleMaster SET IsActive = 0")

    # Set IsActive to 1 for the updated TradeTemplateIDs
    for id in updated_ids:
        conn.execute(f"UPDATE ScheduleMaster SET IsActive = 1 WHERE TradeTemplateID = {id}")

    # Save changes in the database
    conn.commit()
except sqlite3.Error as e:
    print(f"Error occurred while updating IsActive: {str(e)}")
    conn.rollback()
finally:
    # Close the database connection
    conn.close()

# Remove the non-zipped backup
try:
    os.remove(backup_filepath)
except FileNotFoundError:
    print("Warning: Backup file not found. Skipping deletion.")

print("Script executed successfully.")
