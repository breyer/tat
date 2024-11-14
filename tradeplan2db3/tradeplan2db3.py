import pandas as pd
import sqlite3
from datetime import datetime
import shutil
import os
import argparse
import sys
import re

# Argument parser for command line parameters
parser = argparse.ArgumentParser(description='Process tradeplan.')
parser.add_argument('--qty', type=int, help='Set quantity for all entry times')
parser.add_argument('--distribution', action='store_true', help='Distribute contracts based on PnL Rank')
parser.add_argument('--force-initialize', type=int, nargs='?', const=1,
                    help='Force initialize database by deleting existing TradeTemplates and Schedules. Provide the number of plans to initialize (default is 1)')
parser.add_argument('--initialize', action='store_true',
                    help='Initialize database by inserting missing TradeConditions, TradeTemplates, and ScheduleMaster entries without deleting existing data')
args = parser.parse_args()

# Validation: --distribution can only be used if --qty is also provided
if args.distribution and not args.qty:
    print("Error: --distribution can only be set when --qty is also set.")
    sys.exit(1)

# Path to your database file
db_path = 'data.db3'
db_path = os.path.abspath(db_path)
csv_path = 'tradeplan.csv'

# Define the backup directory
backup_dir = 'tradeplan-backup'
backup_dir_path = os.path.abspath(backup_dir)
if not os.path.exists(backup_dir_path):
    try:
        os.makedirs(backup_dir_path)
        print(f"Created backup directory at: {backup_dir_path}")
    except OSError as e:
        print(f"Error: Unable to create backup directory. {str(e)}")
        sys.exit(1)

# Function to create a backup of the database
def create_backup(db_path, backup_dir_path):
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f'data_backup_{current_datetime}'
    backup_filepath = os.path.join(backup_dir_path, backup_filename + '.db3')
    
    try:
        shutil.copy(db_path, backup_filepath)
        print(f"Database backup created at: {backup_filepath}")
        shutil.make_archive(os.path.join(backup_dir_path, backup_filename), 'zip', backup_dir_path, backup_filename + '.db3')
        print(f"Backup ZIP archive created at: {os.path.join(backup_dir_path, backup_filename + '.zip')}")
    except FileNotFoundError:
        print("Error: Database file not found. Please check the file path.")
        sys.exit(1)
    except shutil.Error as e:
        print(f"Error occurred while creating backup: {str(e)}")
        sys.exit(1)
    
    return backup_filepath

backup_filepath = create_backup(db_path, backup_dir_path)

try:
    conn = sqlite3.connect(db_path)
except sqlite3.Error as e:
    print(f"Error occurred while connecting to the database: {str(e)}")
    sys.exit(1)

# Define the times for trade entries
times = [
    "09:33", "09:45", "10:00", "10:15", "10:30", "10:45", "11:00", "11:15", "11:30", "11:45",
    "12:00", "12:15", "12:30", "12:45", "13:00", "13:15", "13:30", "13:45", "14:00", "14:15",
    "14:30", "14:45", "15:00", "15:15", "15:30", "15:45"
]

def create_trade_conditions(conn):
    """
    Create EMA conditions in the database and return their IDs and descriptions.
    Also, set RetryUntilExpiration to 0 for all TradeConditions.
    """
    # Define EMA conditions
    conditions = {
        "EMA520": ("EMA5 > EMA20", ">", "EMA5", "EMA20"),
        "EMA520_INV": ("EMA5 < EMA20", "<", "EMA5", "EMA20"),
        "EMA540": ("EMA5 > EMA40", ">", "EMA5", "EMA40"),
        "EMA540_INV": ("EMA5 < EMA40", "<", "EMA5", "EMA40"),
        "EMA2040": ("EMA20 > EMA40", ">", "EMA20", "EMA40"),
        "EMA2040_INV": ("EMA20 < EMA40", "<", "EMA20", "EMA40"),
        # Add more EMA strategies here if needed
    }

    trade_condition_ids = {}
    for name, (description, operator, input_val, comparison) in conditions.items():
        try:
            # Check if the TradeCondition already exists
            cursor = conn.execute("SELECT TradeConditionID FROM TradeCondition WHERE Name = ?", (description,))
            result = cursor.fetchone()
            if result:
                condition_id = result[0]
                print(f"TradeCondition already exists: {description} with ID {condition_id}")
            else:
                # Insert TradeCondition with RetryUntilExpiration set to 0
                cursor = conn.execute("INSERT INTO TradeCondition (Name, RetryUntilExpiration) VALUES (?, ?)", (description, 0))
                condition_id = cursor.lastrowid
                print(f"Inserted TradeCondition: {description} with ID {condition_id}")

                # Insert TradeConditionDetail with ComparisonType set to 'Input'
                conn.execute("""
                    INSERT INTO TradeConditionDetail (TradeConditionID, [Group], Input, Operator, Comparison, ComparisonType)
                    VALUES (?, 1, ?, ?, ?, 'Input')
                """, (condition_id, input_val, operator, comparison))
                print(f"Inserted TradeConditionDetail for ID {condition_id}")
            
            trade_condition_ids[name] = {
                "id": condition_id,
                "description": description
            }
        except sqlite3.Error as e:
            print(f"Error inserting or fetching TradeCondition '{description}': {str(e)}")
            conn.rollback()
            conn.close()
            sys.exit(1)

    # Set RetryUntilExpiration to 0 for all existing TradeConditions
    try:
        conn.execute("UPDATE TradeCondition SET RetryUntilExpiration = 0")
        print("Set 'RetryUntilExpiration' to 0 for all TradeConditions.")
    except sqlite3.Error as e:
        print(f"Error updating 'RetryUntilExpiration' for TradeConditions: {str(e)}")
        conn.rollback()
        conn.close()
        sys.exit(1)

    # Set ComparisonType to 'Input' for all existing TradeConditionDetail entries
    try:
        conn.execute("UPDATE TradeConditionDetail SET ComparisonType = 'Input'")
        print("Set 'ComparisonType' to 'Input' for all TradeConditionDetail entries.")
    except sqlite3.Error as e:
        print(f"Error updating 'ComparisonType' for TradeConditionDetail: {str(e)}")
        conn.rollback()
        conn.close()
        sys.exit(1)

    print("All default EMA conditions processed successfully.")
    return trade_condition_ids

def create_trade_templates(conn, plan_suffixes):
    """
    Create TradeTemplate entries for PUT and CALL spreads for each plan and time.
    Only inserts templates that do not already exist and ensures IsDeleted is set to 0.
    """
    for plan in plan_suffixes:
        for time in times:
            put_template = {
                "Name": f"PUT SPREAD ({time}) {plan}",
                "TradeType": "PutSpread",
                "TargetType": "Credit",
                "TargetMin": 1.0,
                "TargetMax": 4.0,
                "LongType": "Width",
                "LongWidth": "50",
                "LongMaxPremium": None,
                "QtyDefault": 1,
                "FillAttempts": 5,
                "FillWait": 10,
                "FillAdjustment": 0.05,
                "StopType": "Vertical",
                "StopMultiple": 1.25,
                "StopOffset": 0.0,
                "StopTrigger": 8,
                "StopOrderType": "StopMarket",
                "StopTargetType": "Multiple",
                "StopRelOffset": None,
                "StopRelLimit": None,
                "StopLimitOffset": None,
                "StopLimitMarketOffset": None,
                "OrderIDProfitTarget": "None",
                "ProfitTargetType": None,
                "ProfitTarget": None,
                "Adjustment1Type": "Stop Multiple",
                "Adjustment1": None,
                "Adjustment1ChangeType": "None",
                "Adjustment1Change": 0,
                "Adjustment1ChangeOffset": 0,
                "Adjustment1Hour": 0,
                "Adjustment1Minute": 0,
                "Adjustment2Type": "Stop Multiple",
                "Adjustment2": None,
                "Adjustment2ChangeType": "None",
                "Adjustment2Change": 0,
                "Adjustment2ChangeOffset": 0,
                "Adjustment2Hour": 0,
                "Adjustment2Minute": 0,
                "Adjustment3Type": "Stop Multiple",
                "Adjustment3": None,
                "Adjustment3ChangeType": "None",
                "Adjustment3Change": 0,
                "Adjustment3ChangeOffset": 0,
                "Adjustment3Hour": 0,
                "Adjustment3Minute": 0,
                "ExitHour": 0,
                "ExitMinute": 0,
                "LowerTarget": 0,
                "StopBasis": "Highest Credit/Delta",
                "StopRel": "None",
                "StopRelITM": None,
                "StopRelITMMinutes": 0,
                "LongMaxWidth": 0,
                "ExitMinutesInTrade": None,
                "Preference": "Highest Credit/Delta",
                "Adjustment3Type": "Stop Multiple",
                "Adjustment3": None,
                "Adjustment3ChangeType": "None",
                "Adjustment3Change": 0,
                "Adjustment3ChangeOffset": 0,
                "Adjustment3Hour": 0,
                "Adjustment3Minute": 0,
                "ReEnterClose": 0,
                "ReEnterStop": 0,
                "ReEnterProfitTarget": 0,
                "ReEnterDelay": 0,
                "ReEnterExpirationHour": 0,
                "ReEnterExpirationMinute": 0,
                "ReEnterMaxEntries": 0,
                "DisableNarrowerLong": 0,
                "IsDeleted": 0,
                "Strategy": f"PUT SPREAD {plan}",
                "MinOTM": 0.0,
                "ShortPutTarget": 0.0,
                "ShortPutTargetType": None,
                "ShortPutDTE": 0,
                "ShortCallTarget": 0.0,
                "ShortCallTargetType": None,
                "ShortCallDTE": 0,
                "LongPutTarget": 0.0,
                "LongPutTargetType": None,
                "LongPutDTE": 0,
                "LongCallTarget": 0.0,
                "LongCallTargetType": None,
                "LongCallDTE": 0,
                "ExitDTE": 0,
                "ExtendedHourStop": 0,
                "TargetTypeCall": "Credit",
                "TargetMinCall": 1.0,
                "TargetMaxCall": 4.0,
                "PreferenceCall": "Highest Credit/Delta",
                "MinOTMCall": 0.0,
                "ExitOrderLimit": 0,
                "PutRatio": 1,
                "CallRatio": 1,
                "LongMinPremium": None,
                "ProfitTargetTradePct": 100.0,
                "ProfitTarget2": None,
                "ProfitTarget2TradePct": 100.0,
                "ProfitTarget3": None,
                "ProfitTarget3TradePct": 100.0,
                "ProfitTarget4": None,
                "ProfitTarget4TradePct": 100.0,
                "Adjustment1OrderType": "Same",
                "Adjustment2OrderType": "Same",
                "Adjustment3OrderType": "Same",
                "ReEnterCloseTemplateID": 0,
                "ReEnterStopTemplateID": 0,
                "ReEnterProfitTargetTemplateID": 0,
                "ReEnterCloseTemplateID2": 0,
                "ReEnterStopTemplateID2": 0,
                "ReEnterProfitTargetTemplateID2": 0,
                "MaxEntryPrice": 0.0,
                "MinEntryPrice": 0.0
            }

            # Copy for the CALL spread and modify necessary fields
            call_template = put_template.copy()
            call_template["Name"] = f"CALL SPREAD ({time}) {plan}"
            call_template["TradeType"] = "CallSpread"
            call_template["Strategy"] = f"CALL SPREAD {plan}"

            try:
                # Check if PUT SPREAD template already exists
                cursor = conn.execute("SELECT TradeTemplateID, IsDeleted FROM TradeTemplate WHERE Name = ?", (put_template["Name"],))
                result = cursor.fetchone()
                if result:
                    trade_template_id, is_deleted = result
                    print(f"TradeTemplate already exists: {put_template['Name']} with ID {trade_template_id}")
                    # Ensure IsDeleted is set to 0
                    if is_deleted != 0:
                        conn.execute("UPDATE TradeTemplate SET IsDeleted = 0 WHERE TradeTemplateID = ?", (trade_template_id,))
                        print(f"Updated 'IsDeleted' to 0 for TradeTemplate ID {trade_template_id}: {put_template['Name']}")
                else:
                    # Insert PUT SPREAD template
                    columns = ', '.join(put_template.keys())
                    placeholders = ', '.join(['?'] * len(put_template))
                    values = tuple(put_template.values())
                    conn.execute(f"INSERT INTO TradeTemplate ({columns}) VALUES ({placeholders})", values)
                    print(f"Inserted TradeTemplate: PUT SPREAD ({time}) {plan}")

                # Check if CALL SPREAD template already exists
                cursor = conn.execute("SELECT TradeTemplateID, IsDeleted FROM TradeTemplate WHERE Name = ?", (call_template["Name"],))
                result = cursor.fetchone()
                if result:
                    trade_template_id, is_deleted = result
                    print(f"TradeTemplate already exists: {call_template['Name']} with ID {trade_template_id}")
                    # Ensure IsDeleted is set to 0
                    if is_deleted != 0:
                        conn.execute("UPDATE TradeTemplate SET IsDeleted = 0 WHERE TradeTemplateID = ?", (trade_template_id,))
                        print(f"Updated 'IsDeleted' to 0 for TradeTemplate ID {trade_template_id}: {call_template['Name']}")
                else:
                    # Insert CALL SPREAD template
                    columns = ', '.join(call_template.keys())
                    placeholders = ', '.join(['?'] * len(call_template))
                    values = tuple(call_template.values())
                    conn.execute(f"INSERT INTO TradeTemplate ({columns}) VALUES ({placeholders})", values)
                    print(f"Inserted TradeTemplate: CALL SPREAD ({time}) {plan}")

            except sqlite3.Error as e:
                print(f"Error inserting TradeTemplate for {time} {plan}: {str(e)}")
                conn.rollback()
                conn.close()
                sys.exit(1)

def create_schedules(conn, plan_suffixes, trade_condition_ids, accounts, active=True):
    """
    Create ScheduleMaster entries linked to TradeTemplates with appropriate EMA conditions.
    Only inserts schedules that do not already exist.
    Each account will have its own ScheduleMaster for each TradeTemplate.
    
    Parameters:
        active (bool): Determines if the schedule should be active (True) or inactive (False).
    """
    for plan in plan_suffixes:
        for time in times:
            hour, minute = map(int, time.split(':'))

            # Get TradeTemplateID for PUT SPREAD
            put_template_name = f"PUT SPREAD ({time}) {plan}"
            cursor = conn.execute("""
                SELECT TradeTemplateID FROM TradeTemplate
                WHERE Name = ?
            """, (put_template_name,))
            result = cursor.fetchone()
            if result:
                put_template_id = result[0]
            else:
                print(f"Error: PUT SPREAD TradeTemplate '{put_template_name}' not found.")
                conn.rollback()
                conn.close()
                sys.exit(1)

            # Get TradeTemplateID for CALL SPREAD
            call_template_name = f"CALL SPREAD ({time}) {plan}"
            cursor = conn.execute("""
                SELECT TradeTemplateID FROM TradeTemplate
                WHERE Name = ?
            """, (call_template_name,))
            result = cursor.fetchone()
            if result:
                call_template_id = result[0]
            else:
                print(f"Error: CALL SPREAD TradeTemplate '{call_template_name}' not found.")
                conn.rollback()
                conn.close()
                sys.exit(1)

            for account in accounts:
                try:
                    # Define ScheduleType
                    schedule_type = "Trade"

                    # Mapping strategies to their corresponding condition IDs and descriptions
                    # Assuming trade_condition_ids contains all necessary mappings
                    # For PUT spreads
                    put_strategy = 'EMA520'  # Adjust based on actual strategy
                    put_trade_condition = trade_condition_ids.get(put_strategy)
                    if not put_trade_condition:
                        print(f"Error: TradeCondition for strategy '{put_strategy}' not found.")
                        conn.rollback()
                        conn.close()
                        sys.exit(1)
                    display_strategy_put = f"PUT SPREAD {plan}"
                    display_condition_put = put_trade_condition["description"]

                    # For CALL spreads
                    call_strategy = 'EMA520_INV'  # Adjust based on actual strategy
                    call_trade_condition = trade_condition_ids.get(call_strategy)
                    if not call_trade_condition:
                        print(f"Error: TradeCondition for strategy '{call_strategy}' not found.")
                        conn.rollback()
                        conn.close()
                        sys.exit(1)
                    display_strategy_call = f"CALL SPREAD {plan}"
                    display_condition_call = call_trade_condition["description"]

                    # Insert ScheduleMaster for PUT SPREAD
                    cursor = conn.execute("""
                        SELECT ScheduleMasterID FROM ScheduleMaster
                        WHERE TradeTemplateID = ? AND Strategy = ? AND Account = ?
                    """, (put_template_id, put_strategy, account))
                    if cursor.fetchone():
                        print(f"ScheduleMaster already exists for PUT SPREAD '{put_template_name}' with Strategy {put_strategy} and Account '{account}'")
                    else:
                        # Insert ScheduleMaster for PUT SPREAD
                        conn.execute("""
                            INSERT INTO ScheduleMaster (
                                Account, TradeTemplateID, ScheduleType, QtyOverride,
                                Hour, Minute, Second, ExpirationMinutes, IsActive,
                                ScheduleGroupID, Condition, Strategy, DisplayStrategy,
                                TradeConditionID, DisplayCondition,
                                DayMonday, DayTuesday, DayWednesday, DayThursday, DayFriday, DaySunday,
                                QtyType, QtyAllocation, QtyAllocationMax
                            ) VALUES (
                                ?, ?, ?, ?,
                                ?, ?, ?, ?, ?,
                                ?, ?, ?, ?,
                                ?, ?,
                                ?, ?, ?, ?, ?, ?,
                                ?, ?, ?
                            )
                        """, (
                            account, put_template_id, schedule_type, 1,
                            hour, minute, 0, 5, int(active),  # Set IsActive based on the 'active' parameter
                            0, None, put_strategy, display_strategy_put,
                            put_trade_condition["id"], display_condition_put,
                            1, 1, 1, 1, 1, 0,  # DayMonday to DaySunday
                            "FixedQty", 0.0, 0  # QtyType, QtyAllocation, QtyAllocationMax
                        ))
                        status = "active" if active else "inactive"
                        print(f"Inserted ScheduleMaster for PUT SPREAD '{put_template_name}' with Strategy {put_strategy} and Account '{account}' as {status}.")

                    # Insert ScheduleMaster for CALL SPREAD
                    cursor = conn.execute("""
                        SELECT ScheduleMasterID FROM ScheduleMaster
                        WHERE TradeTemplateID = ? AND Strategy = ? AND Account = ?
                    """, (call_template_id, call_strategy, account))
                    if cursor.fetchone():
                        print(f"ScheduleMaster already exists for CALL SPREAD '{call_template_name}' with Strategy {call_strategy} and Account '{account}'")
                    else:
                        # Insert ScheduleMaster for CALL SPREAD
                        conn.execute("""
                            INSERT INTO ScheduleMaster (
                                Account, TradeTemplateID, ScheduleType, QtyOverride,
                                Hour, Minute, Second, ExpirationMinutes, IsActive,
                                ScheduleGroupID, Condition, Strategy, DisplayStrategy,
                                TradeConditionID, DisplayCondition,
                                DayMonday, DayTuesday, DayWednesday, DayThursday, DayFriday, DaySunday,
                                QtyType, QtyAllocation, QtyAllocationMax
                            ) VALUES (
                                ?, ?, ?, ?,
                                ?, ?, ?, ?, ?,
                                ?, ?, ?, ?,
                                ?, ?,
                                ?, ?, ?, ?, ?, ?,
                                ?, ?, ?
                            )
                        """, (
                            account, call_template_id, schedule_type, 1,
                            hour, minute, 0, 5, int(active),  # Set IsActive based on the 'active' parameter
                            0, None, call_strategy, display_strategy_call,
                            call_trade_condition["id"], display_condition_call,
                            1, 1, 1, 1, 1, 0,  # DayMonday to DaySunday
                            "FixedQty", 0.0, 0  # QtyType, QtyAllocation, QtyAllocationMax
                        ))
                        status = "active" if active else "inactive"
                        print(f"Inserted ScheduleMaster for CALL SPREAD '{call_template_name}' with Strategy {call_strategy} and Account '{account}' as {status}.")

                except sqlite3.Error as e:
                    print(f"Error inserting ScheduleMaster for {time} {plan} with Account '{account}': {str(e)}")
                    conn.rollback()
                    conn.close()
                    sys.exit(1)

def initialize_database(conn, plan_count, force, accounts):
    """
    Initialize the database with TradeConditions, TradeTemplates, and ScheduleMaster entries.
    If force is True, it deletes existing entries and creates schedules as inactive.
    Otherwise, it inserts missing entries without deleting.
    """
    try:
        if force:
            # Clear existing records in relevant tables
            conn.execute("DELETE FROM TradeTemplate")
            conn.execute("DELETE FROM ScheduleMaster")
            conn.execute("DELETE FROM TradeConditionDetail")
            conn.execute("DELETE FROM TradeCondition")
            conn.commit()
            print("Cleared existing TradeTemplates, Schedules, and TradeConditions.")

            # Reset the TradeCondition sequence if using AUTOINCREMENT
            # This ensures that new TradeCondition IDs start from 1
            # Note: This only works if the TradeCondition table uses AUTOINCREMENT
            try:
                conn.execute("DELETE FROM sqlite_sequence WHERE name='TradeCondition'")
                conn.commit()
                print("Reset TradeCondition ID sequence.")
            except sqlite3.Error as e:
                print(f"Warning: Could not reset TradeCondition ID sequence. {str(e)}")
                # Proceeding without resetting sequence

        else:
            # Initialize only missing TradeConditions, TradeTemplates, and ScheduleMaster entries
            print("Initializing database by inserting missing TradeConditions, TradeTemplates, and ScheduleMaster entries.")

        # Create EMA conditions and get their IDs
        trade_condition_ids = create_trade_conditions(conn)

        # Initialize TradeTemplates and associate with schedules
        if force or args.initialize:
            if force:
                # If force initializing, ensure all plans are initialized
                plan_suffixes = [f"P{i}" for i in range(1, plan_count + 1)]
                active_status = False  # Schedules should be inactive
            else:
                # If not force, assume plan_count=1 for non-forced initialize
                plan_suffixes = ['P1']
                active_status = True  # Schedules can be active

            create_trade_templates(conn, plan_suffixes)

            if force:
                if not accounts:
                    print("Error: No accounts provided for force-initialize.")
                    conn.rollback()
                    conn.close()
                    sys.exit(1)
                # Pass active=False to deactivate schedules
                create_schedules(conn, plan_suffixes, trade_condition_ids, accounts, active=active_status)
            else:
                # For non-force initialize, you might want to handle differently
                # Here, we'll assume accounts are already set up
                pass

        conn.commit()
        if force:
            print(f"Initialized database with TradeTemplates for plans P1 to P{plan_count}, and associated Schedules as inactive.")
        elif args.initialize:
            print("Inserted missing TradeTemplates and ScheduleMaster entries.")
        else:
            print("Database initialization skipped.")

    except sqlite3.Error as e:
        print(f"Error occurred during initialization: {str(e)}")
        conn.rollback()
        conn.close()
        sys.exit(1)

def get_accounts():
    """
    Prompt the user to input Account IDs in the format "IB:U1234567" or "IB:U12345678".
    Allows up to 3 accounts.
    """
    accounts = []
    max_accounts = 3
    example_account = "IB:U1234567 or IB:U12345678"

    while len(accounts) < max_accounts:
        account_input = input(f"Enter Account ID (e.g., {example_account}): ").strip()
        if not account_input:
            print("Account ID cannot be empty. Please try again.")
            continue

        # Define regex patterns for different input formats
        patterns = [
            r'^IB:U\d{7,8}$',      # IB:U1234567 or IB:U12345678
            r'^IB:\d{7,8}$',        # IB:1234567 or IB:12345678
            r'^U\d{7,8}$',          # U1234567 or U12345678
            r'^\d{7,8}$'            # 1234567 or 12345678
        ]

        matched = False
        for pattern in patterns:
            if re.match(pattern, account_input):
                matched = True
                break

        if not matched:
            print(f"Invalid format for Account ID. Please enter in the format 'IB:U########' (e.g., {example_account}).")
            continue

        # Normalize the account input to "IB:U########"
        if account_input.startswith("IB:U") and len(account_input) in [11, 12]:
            formatted_account = account_input
        elif account_input.startswith("IB:") and len(account_input) in [10, 11]:
            # Missing 'U', add it
            formatted_account = f"IB:U{account_input[4:]}"
        elif account_input.startswith("U") and len(account_input) in [8, 9]:
            # Missing 'IB:', add it
            formatted_account = f"IB:{account_input}"
        elif len(account_input) in [7, 8] and account_input.isdigit():
            # Missing 'IB:U', add both
            formatted_account = f"IB:U{account_input}"
        else:
            # This should not happen due to regex, but added for safety
            print(f"Invalid format for Account ID. Please enter in the format 'IB:U########' (e.g., {example_account}).")
            continue

        accounts.append(formatted_account)
        print(f"Added Account: {formatted_account}")

        if len(accounts) < max_accounts:
            add_more = input("Do you want to add another account? (y/n): ").strip().lower()
            if add_more != 'y':
                break

    if not accounts:
        print("No valid accounts provided. Exiting.")
        sys.exit(1)

    return accounts

def process_tradeplan(conn, data):
    """
    Process the tradeplan.csv and update TradeTemplates and ScheduleMaster accordingly.
    """
    # Determine if "Plan" column exists
    if "Plan" in data.columns:
        # Use the provided Plan values
        data['Plan'] = data['Plan'].str.upper()
        unique_plans = data['Plan'].unique().tolist()
        print(f"Processing plans: {', '.join(unique_plans)}")
    else:
        # Default all entries to 'P1'
        data['Plan'] = 'P1'
        unique_plans = ['P1']
        print("No 'Plan' column found in the tradeplan.csv. Treating all entries as Plan P1.")

    # Fetch TradeConditionIDs and descriptions from the database
    trade_condition_ids = {}
    cursor = conn.execute("SELECT TradeConditionID, Name FROM TradeCondition")
    for row in cursor.fetchall():
        condition_id, name = row
        for strategy_code, details in trade_condition_ids.items():
            # Assuming strategy_code maps to condition name
            if details["description"] == name:
                trade_condition_ids[strategy_code]["id"] = condition_id

    # Alternatively, fetch the mapping correctly
    trade_condition_ids = {}
    cursor = conn.execute("SELECT TradeConditionID, Name FROM TradeCondition")
    for row in cursor.fetchall():
        condition_id, name = row
        for strategy_code, details in create_trade_conditions(conn).items():
            if details["description"] == name:
                trade_condition_ids[strategy_code] = {
                    "id": condition_id,
                    "description": name
                }

    # Determine required conditions based on strategies in the CSV
    required_conditions = set()
    for strategy in data['Strategy'].unique():
        strategy_upper = strategy.upper()
        if strategy_upper in ["EMA520", "EMA540", "EMA2040"]:  # Add more strategies as needed
            required_conditions.add(strategy_upper)
            required_conditions.add(f"{strategy_upper}_INV")
        else:
            print(f"Error: Unsupported Strategy '{strategy}' found in CSV.")
            print("Please ensure all strategies are supported or update the script accordingly.")
            conn.close()
            sys.exit(1)

    # Check for missing TradeConditionIDs
    missing_conditions = required_conditions - set(trade_condition_ids.keys())
    if missing_conditions:
        missing_str = ', '.join(missing_conditions)
        print(f"\nWARNING: Missing TradeConditionIDs for strategies: {missing_str}")
        print("Please run the script with '--force-initialize' to create the necessary TradeConditions.")
        conn.close()
        sys.exit(1)

    # Determine all required TradeTemplate names based on the CSV
    required_trade_templates = set()
    for _, row in data.iterrows():
        plan = row['Plan']
        time = row['Hour:Minute']
        required_trade_templates.add(f"PUT SPREAD ({time}) {plan}")
        required_trade_templates.add(f"CALL SPREAD ({time}) {plan}")

    # Fetch existing TradeTemplate names from the database
    cursor = conn.execute("SELECT Name FROM TradeTemplate")
    existing_trade_templates = set(name for (name,) in cursor.fetchall())

    # Determine missing TradeTemplates
    missing_trade_templates = required_trade_templates - existing_trade_templates
    if missing_trade_templates:
        missing_templates_str = ', '.join(missing_trade_templates)
        print(f"\nWARNING: Missing TradeTemplates: {missing_templates_str}")
        print("Please run the script with '--force-initialize' to create the necessary TradeTemplates.")
        conn.close()
        sys.exit(1)

    # Iterate over each row in the CSV
    for index, row in data.iterrows():
        plan = row['Plan']
        hour_minute = row['Hour:Minute']
        premium = float(row['Premium'])
        # Replace dashes with commas in 'Spread'
        spread = row['Spread'].replace('-', ',')
        stop_str = str(row['Stop'])
        if stop_str.endswith('x') or stop_str.endswith('X'):
            stop_multiple = float(stop_str[:-1])
        else:
            try:
                stop_multiple = float(stop_str)
            except ValueError:
                print(f"Error: Invalid Stop value '{stop_str}' at row {index + 1}.")
                conn.close()
                sys.exit(1)

        qty_override = int(row['Qty'])
        ema_strategy = row['Strategy'].upper()

        # Determine the TradeConditionID and DisplayCondition based on the strategy
        if ema_strategy == "EMA520":
            condition_id_put = trade_condition_ids.get("EMA520", {}).get("id")
            condition_name_put = trade_condition_ids.get("EMA520", {}).get("description", "EMA5 > EMA20")
            condition_id_call = trade_condition_ids.get("EMA520_INV", {}).get("id")
            condition_name_call = trade_condition_ids.get("EMA520_INV", {}).get("description", "EMA5 < EMA20")
        elif ema_strategy == "EMA540":
            condition_id_put = trade_condition_ids.get("EMA540", {}).get("id")
            condition_name_put = trade_condition_ids.get("EMA540", {}).get("description", "EMA5 > EMA40")
            condition_id_call = trade_condition_ids.get("EMA540_INV", {}).get("id")
            condition_name_call = trade_condition_ids.get("EMA540_INV", {}).get("description", "EMA5 < EMA40")
        elif ema_strategy == "EMA2040":
            condition_id_put = trade_condition_ids.get("EMA2040", {}).get("id")
            condition_name_put = trade_condition_ids.get("EMA2040", {}).get("description", "EMA20 > EMA40")
            condition_id_call = trade_condition_ids.get("EMA2040_INV", {}).get("id")
            condition_name_call = trade_condition_ids.get("EMA2040_INV", {}).get("description", "EMA20 < EMA40")
        else:
            print(f"Error: Unknown Strategy '{ema_strategy}' at row {index + 1}.")
            conn.close()
            sys.exit(1)

        # Update PUT SPREAD TradeTemplate
        put_template_name = f"PUT SPREAD ({hour_minute}) {plan}"
        cursor = conn.execute("""
            SELECT TradeTemplateID FROM TradeTemplate
            WHERE Name = ?
        """, (put_template_name,))
        result = cursor.fetchone()
        if result:
            put_template_id = result[0]

            # Update only TargetMax, LongWidth, and StopMultiple
            try:
                conn.execute("""
                    UPDATE TradeTemplate
                    SET TargetMax = ?, LongWidth = ?, StopMultiple = ?
                    WHERE TradeTemplateID = ?
                """, (premium, spread, stop_multiple, put_template_id))
                print(f"Updated TradeTemplate for PUT SPREAD '{put_template_name}'.")
            except sqlite3.Error as e:
                print(f"Error updating TradeTemplate for PUT SPREAD '{put_template_name}': {str(e)}")
                conn.rollback()
                conn.close()
                sys.exit(1)

            # Update ScheduleMaster for PUT SPREAD
            try:
                conn.execute("""
                    UPDATE ScheduleMaster
                    SET IsActive = 1, QtyOverride = ?, Strategy = ?, TradeConditionID = ?, DisplayStrategy = ?, DisplayCondition = ?
                    WHERE TradeTemplateID = ?
                """, (qty_override, ema_strategy, condition_id_put, f"PUT SPREAD {plan}", condition_name_put, put_template_id))
                print(f"Updated ScheduleMaster for PUT SPREAD '{put_template_name}'.")
            except sqlite3.Error as e:
                print(f"Error updating ScheduleMaster for PUT SPREAD '{put_template_name}': {str(e)}")
                conn.rollback()
                conn.close()
                sys.exit(1)
        else:
            print(f"\nWARNING: PUT SPREAD TradeTemplate '{put_template_name}' not found.")
            print("Please run the script with '--force-initialize' to create the necessary TradeTemplates.")
            conn.rollback()
            conn.close()
            sys.exit(1)

        # Update CALL SPREAD TradeTemplate
        call_template_name = f"CALL SPREAD ({hour_minute}) {plan}"
        cursor = conn.execute("""
            SELECT TradeTemplateID FROM TradeTemplate
            WHERE Name = ?
        """, (call_template_name,))
        result = cursor.fetchone()
        if result:
            call_template_id = result[0]

            # Update only TargetMax, LongWidth, and StopMultiple
            try:
                conn.execute("""
                    UPDATE TradeTemplate
                    SET TargetMax = ?, LongWidth = ?, StopMultiple = ?
                    WHERE TradeTemplateID = ?
                """, (premium, spread, stop_multiple, call_template_id))
                print(f"Updated TradeTemplate for CALL SPREAD '{call_template_name}'.")
            except sqlite3.Error as e:
                print(f"Error updating TradeTemplate for CALL SPREAD '{call_template_name}': {str(e)}")
                conn.rollback()
                conn.close()
                sys.exit(1)

            # Update ScheduleMaster for CALL SPREAD
            try:
                conn.execute("""
                    UPDATE ScheduleMaster
                    SET IsActive = 1, QtyOverride = ?, Strategy = ?, TradeConditionID = ?, DisplayStrategy = ?, DisplayCondition = ?
                    WHERE TradeTemplateID = ?
                """, (qty_override, f"{ema_strategy}_INV", condition_id_call, f"CALL SPREAD {plan}", condition_name_call, call_template_id))
                print(f"Updated ScheduleMaster for CALL SPREAD '{call_template_name}'.")
            except sqlite3.Error as e:
                print(f"Error updating ScheduleMaster for CALL SPREAD '{call_template_name}': {str(e)}")
                conn.rollback()
                conn.close()
                sys.exit(1)
        else:
            print(f"\nWARNING: CALL SPREAD TradeTemplate '{call_template_name}' not found.")
            print("Please run the script with '--force-initialize' to create the necessary TradeTemplates.")
            conn.rollback()
            conn.close()
            sys.exit(1)

    # Commit all updates
    try:
        conn.commit()
        print("\nTradeTemplates and ScheduleMaster entries updated successfully.")
    except sqlite3.Error as e:
        print(f"Error occurred while committing updates: {str(e)}")
        conn.rollback()
        conn.close()
        sys.exit(1)

def main():
    if args.force_initialize is not None:
        # Determine the number of plans to initialize
        if isinstance(args.force_initialize, int):
            plan_count = args.force_initialize
        else:
            plan_count = 1  # Default to 1 plan if no number is provided

        # Prompt user for Account IDs
        print("Force initializing the database. Please provide Account IDs.")
        accounts = get_accounts()

        initialize_database(conn, plan_count, force=True, accounts=accounts)
        conn.close()
        sys.exit(0)

    if args.initialize:
        # Determine the number of plans to initialize; assuming 1 if not specified
        plan_count = 1
        # For non-force initialize, accounts are not required
        initialize_database(conn, plan_count, force=False, accounts=[])
        conn.close()
        sys.exit(0)

    # Read the CSV file
    try:
        data = pd.read_csv(csv_path, delimiter=',', quotechar='"')
    except FileNotFoundError:
        print("Error: CSV file not found. Please check the file path.")
        conn.close()
        sys.exit(1)
    except pd.errors.EmptyDataError:
        print("Error: CSV file is empty.")
        conn.close()
        sys.exit(1)
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV file: {str(e)}")
        conn.close()
        sys.exit(1)

    # If --qty is specified, set the Qty column accordingly
    if args.qty:
        data['Qty'] = args.qty
        print(f"Set Qty for all entries to {args.qty}.")

    # If --distribution is specified, adjust Qty based on PnL Rank
    if args.distribution:
        if 'PnL Rank' not in data.columns:
            print("Error: 'PnL Rank' column is missing in the CSV file. Cannot distribute contracts.")
            conn.close()
            sys.exit(1)
        data = data.sort_values('PnL Rank')
        data['Qty'] = data['Qty'].fillna(0)  # Fill NaN values with 0 for safety

        # Add 1 to top 1-3 PnL Rank
        top_n = 3
        data.loc[data.index[:top_n], 'Qty'] += 1
        print(f"Added 1 to Qty for top {top_n} PnL Rank entries.")

        # Subtract 1 from 8-10 PnL Rank if there are at least 10 entries
        subtract_n = 3
        if len(data) >= 10:
            data.loc[data.index[7:10], 'Qty'] -= 1
            print(f"Subtracted 1 from Qty for PnL Rank entries 8-10.")

    # Replace dashes with commas in 'Spread' and save back to CSV
    if 'Spread' in data.columns:
        data['Spread'] = data['Spread'].astype(str).str.replace('-', ',', regex=False)
        print("Replaced dashes with commas in 'Spread' column.")

    # Save the updated CSV back to file
    data.to_csv(csv_path, index=False)
    print("Updated tradeplan.csv saved.")

    # Deactivate all schedules
    try:
        conn.execute("UPDATE ScheduleMaster SET IsActive = 0")
        conn.commit()
        print("All schedules deactivated.")
    except sqlite3.Error as e:
        print(f"Error occurred while deactivating schedules: {str(e)}")
        conn.rollback()
        conn.close()
        sys.exit(1)

    # Process the tradeplan.csv to update TradeTemplates and ScheduleMaster
    process_tradeplan(conn, data)

    # Close the database connection
    try:
        conn.close()
    except sqlite3.Error as e:
        print(f"Error occurred while closing the database: {str(e)}")

    # Remove the non-zipped backup file after successful processing
    try:
        os.remove(backup_filepath)
        print(f"Removed unzipped backup file: {backup_filepath}")
    except FileNotFoundError:
        print("Warning: Backup file not found. Skipping deletion.")

    print("Script executed successfully.\n")
    print("Final tradeplan.csv:")
    print(data.to_csv(index=False))

if __name__ == "__main__":
    main()
