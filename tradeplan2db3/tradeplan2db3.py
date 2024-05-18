#! python
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

# Copy the original database file to the backup path
shutil.copy(db_path, backup_filepath)

# Create a ZIP archive of the backup
shutil.make_archive(backup_filename, 'zip', '.', backup_filepath)

# Establish a connection to the SQLite database
conn = sqlite3.connect(db_path)

# Read data from the CSV file
data = pd.read_csv(csv_path, delimiter=',', quotechar='"')

# List to store the updated TradeTemplateIDs
updated_ids = []

# Making changes in the database
for index, row in data.iterrows():
    hour_minute = row['Hour:Minute']
    premium = row['Premium']
    spread = row['Spread']
    stop = row['Stop']
    strategy = row['Strategy']

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

    # Update Conditions and Strategy in ScheduleMaster
    for trade_template_id in put_ids:
        if strategy == 'EMA540':
            conn.execute(f"UPDATE ScheduleMaster SET Condition = 'EMA5 > EMA40', Strategy = '{strategy}' WHERE TradeTemplateID = {trade_template_id}")
        elif strategy == 'EMA520':
            conn.execute(f"UPDATE ScheduleMaster SET Condition = 'EMA5 > EMA20', Strategy = '{strategy}' WHERE TradeTemplateID = {trade_template_id}")
        elif strategy == 'EMA2040':
            conn.execute(f"UPDATE ScheduleMaster SET Condition = 'EMA20 > EMA40', Strategy = '{strategy}' WHERE TradeTemplateID = {trade_template_id}")
    
    for trade_template_id in call_ids:
        if strategy == 'EMA540':
            conn.execute(f"UPDATE ScheduleMaster SET Condition = 'EMA5 < EMA40', Strategy = '{strategy}' WHERE TradeTemplateID = {trade_template_id}")
        elif strategy == 'EMA520':
            conn.execute(f"UPDATE ScheduleMaster SET Condition = 'EMA5 < EMA20', Strategy = '{strategy}' WHERE TradeTemplateID = {trade_template_id}")
        elif strategy == 'EMA2040':
            conn.execute(f"UPDATE ScheduleMaster SET Condition = 'EMA20 < EMA40', Strategy = '{strategy}' WHERE TradeTemplateID = {trade_template_id}")

# Set IsActive to 0 in the ScheduleMaster table
conn.execute("UPDATE ScheduleMaster SET IsActive = 0")

# Set IsActive to 1 for the updated TradeTemplateIDs
for id in updated_ids:
    conn.execute(f"UPDATE ScheduleMaster SET IsActive = 1 WHERE TradeTemplateID = {id}")

# Save changes in the database
conn.commit()

# Close the database connection
conn.close()

# Remove the non-zipped backup
os.remove(backup_filepath)
