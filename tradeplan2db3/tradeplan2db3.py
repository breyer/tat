#!/usr/bin/env python3
"""
tradeplan2db3.py

This script processes a trade plan from a CSV file and updates an SQLite database accordingly.
It handles database backups, initializes trade conditions, trade templates, and schedules based
on the provided CSV and command-line arguments.

Usage:
    python tradeplan2db3.py [--qty QTY] [--distribution] [--force-initialize [PLAN_COUNT]]
                            [--initialize]
"""

import argparse
import logging
import os
import re
import shutil
import sqlite3
import sys
from datetime import datetime

import pandas as pd
import numpy as np # Import numpy for np.nan


def parse_arguments():
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(description='Process tradeplan.')
    parser.add_argument(
        '--qty',
        type=int,
        help='Set quantity for all entry times'
    )
    parser.add_argument(
        '--distribution',
        action='store_true',
        help='Distribute contracts based on PnL Rank'
    )
    parser.add_argument(
        '--force-initialize',
        type=int,
        nargs='?',
        const=-1,  # Sentinel value to indicate no number provided
        help=(
            'Force initialize database by deleting existing TradeTemplates and '
            'Schedules. If no number is provided, the script will prompt for one.'
        )
    )
    parser.add_argument(
        '--initialize',
        action='store_true',
        help=(
            'Initialize database by inserting missing TradeConditions, '
            'TradeTemplates, and ScheduleMaster entries without deleting existing data'
        )
    )
    return parser.parse_args()


def setup_logging():
    """
    Configure logging for the script.
    """
    logging.basicConfig(
        filename='tradeplan_updates.log',
        filemode='a',  # Append mode
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


def create_backup(db_path, backup_dir_path):
    """
    Create a backup of the database and compress it into a ZIP archive.
    """
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f'data_backup_{current_datetime}'
    backup_filepath = os.path.join(backup_dir_path, backup_filename + '.db3')
    backup_zip_path = os.path.join(backup_dir_path, backup_filename + '.zip')

    try:
        shutil.copy(db_path, backup_filepath)
        logging.info(f"Database backup created at: {backup_filepath}")
        shutil.make_archive(
            os.path.join(backup_dir_path, backup_filename),
            'zip',
            backup_dir_path,
            os.path.basename(backup_filepath)
        )
        logging.info(f"Backup ZIP archive created at: {backup_zip_path}")
    except FileNotFoundError:
        logging.error("Database file not found. Please check the file path.")
        print("Error: Database file not found. Please check the file path.")
        sys.exit(1)
    except shutil.Error as e:
        logging.error(f"Error occurred while creating backup: {str(e)}")
        print(f"Error occurred while creating backup: {str(e)}")
        sys.exit(1)

    return backup_filepath


def connect_database(db_path):
    """
    Connect to the SQLite database.
    """
    try:
        conn = sqlite3.connect(db_path)
        logging.info(f"Connected to database at {db_path}.")
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error occurred while connecting to the database: {str(e)}")
        print(f"Error occurred while connecting to the database: {str(e)}")
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

        patterns = [
            r'^IB:U\d{7,8}$',  # IB:U1234567 or IB:U12345678
            r'^IB:\d{7,8}$',   # IB:1234567 or IB:12345678
            r'^U\d{7,8}$',     # U1234567 or U12345678
            r'^\d{7,8}$'       # 1234567 or 12345678
        ]
        matched = any(re.match(pattern, account_input) for pattern in patterns)

        if not matched:
            print(
                f"Invalid format for Account ID. Please enter in the format 'IB:U########' "
                f"(e.g., {example_account})."
            )
            continue

        # Normalize
        if account_input.startswith("IB:U") and len(account_input) in [11, 12]:
            formatted_account = account_input
        elif account_input.startswith("IB:") and len(account_input) in [10, 11]:
            # Missing 'U', add it
            formatted_account = f"IB:U{account_input[3:]}" # Corrected index
        elif account_input.startswith("U") and len(account_input) in [8, 9]:
            # Missing 'IB:', add it
            formatted_account = f"IB:{account_input}"
        elif len(account_input) in [7, 8] and account_input.isdigit():
            # Missing 'IB:U'
            formatted_account = f"IB:U{account_input}"
        else:
            print(
                f"Invalid format for Account ID. Please enter in the format 'IB:U########' "
                f"(e.g., {example_account})."
            )
            continue

        accounts.append(formatted_account)
        print(f"Added Account: {formatted_account}")
        logging.info(f"Added Account: {formatted_account}")

        if len(accounts) < max_accounts:
            add_more = input("Do you want to add another account? (y/n): ").strip().lower()
            if add_more != 'y':
                break

    if not accounts:
        print("No valid accounts provided. Exiting.")
        logging.error("No valid accounts provided.")
        sys.exit(1)

    return accounts


def create_trade_conditions(conn):
    """
    Create EMA conditions in the database and return their IDs and descriptions.
    Also set RetryUntilExpiration=0 for all conditions.
    """
    conditions = {
        "EMA520": ("EMA5 > EMA20", ">", "EMA5", "EMA20"),
        "EMA520_INV": ("EMA5 < EMA20", "<", "EMA5", "EMA20"),
        "EMA540": ("EMA5 > EMA40", ">", "EMA5", "EMA40"),
        "EMA540_INV": ("EMA5 < EMA40", "<", "EMA5", "EMA40"),
        "EMA2040": ("EMA20 > EMA40", ">", "EMA20", "EMA40"),
        "EMA2040_INV": ("EMA20 < EMA40", "<", "EMA20", "EMA40"),
    }

    trade_condition_ids = {}
    try:
        for name, (description, operator, input_val, comparison) in conditions.items():
            # Check if existing
            cursor = conn.execute(
                "SELECT TradeConditionID FROM TradeCondition WHERE Name = ?",
                (description,)
            )
            result = cursor.fetchone()
            if result:
                condition_id = result[0]
                logging.info(
                    f"TradeCondition already exists: {description} with ID {condition_id}"
                )
            else:
                cursor = conn.execute(
                    "INSERT INTO TradeCondition (Name, RetryUntilExpiration) VALUES (?, ?)",
                    (description, 0)
                )
                condition_id = cursor.lastrowid
                logging.info(
                    f"Inserted TradeCondition: {description} with ID {condition_id}"
                )

                # Detail
                conn.execute("""
                    INSERT INTO TradeConditionDetail (
                        TradeConditionID, [Group], Input, Operator, Comparison, ComparisonType
                    ) VALUES (?, 1, ?, ?, ?, 'Input')
                """, (condition_id, input_val, operator, comparison))
                logging.info(f"Inserted TradeConditionDetail for ID {condition_id}")

            trade_condition_ids[name] = {
                "id": condition_id,
                "description": description
            }

        # Update RetryUntilExpiration for all
        conn.execute("UPDATE TradeCondition SET RetryUntilExpiration = 0")
        logging.info("Set 'RetryUntilExpiration' to 0 for all TradeConditions.")

        # Update ComparisonType for all
        conn.execute("UPDATE TradeConditionDetail SET ComparisonType = 'Input'")
        logging.info("Set 'ComparisonType' to 'Input' for all TradeConditionDetail entries.")

        logging.info("All default EMA conditions processed successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error inserting or updating TradeConditions: {str(e)}")
        raise

    return trade_condition_ids


def create_trade_templates(conn, plan_suffixes, times):
    """
    Create or update TradeTemplates for given plan_suffixes and times.
    Default ProfitTargetType and ProfitTarget are None and will be updated by process_tradeplan if specified in CSV.
    """
    try:
        for plan in plan_suffixes:
            for time in times:
                # Define the base template structure
                base_template = {
                    "Name": "", # To be set
                    "TradeType": "", # To be set
                    "TargetType": "Credit",
                    "TargetMin": 1.0,
                    "TargetMax": 4.0, # Default for PUT, will be 0.0 for CALL
                    "LongType": "Width",
                    "LongWidth": "20,25,30",
                    "LongMaxPremium": None,
                    "QtyDefault": 1,
                    "FillAttempts": 5,
                    "FillWait": 15,
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
                    "OrderIDProfitTarget": "None", # Default, updated if profit target is set
                    "ProfitTargetType": None,      # Default, updated by process_tradeplan
                    "ProfitTarget": None,          # Default, updated by process_tradeplan
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
                    "ReEnterClose": 0,
                    "ReEnterStop": 0,
                    "ReEnterProfitTarget": 0,
                    "ReEnterDelay": 0,
                    "ReEnterExpirationHour": 0,
                    "ReEnterExpirationMinute": 0,
                    "ReEnterMaxEntries": 0,
                    "DisableNarrowerLong": 0,
                    "IsDeleted": 0,
                    "Strategy": "", # To be set
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
                    "TargetTypeCall": "Credit", # Specific to CALL, but part of the table structure
                    "TargetMinCall": 1.0,      # Specific to CALL
                    "TargetMaxCall": 4.0,      # Specific to CALL, will be updated
                    "PreferenceCall": "Highest Credit/Delta", # Specific to CALL
                    "MinOTMCall": 0.0,         # Specific to CALL
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

                # PUT Template
                put_template = base_template.copy()
                put_template["Name"] = f"PUT SPREAD ({time}) {plan}"
                put_template["TradeType"] = "PutSpread"
                put_template["Strategy"] = f"PUT SPREAD {plan}"
                # TargetMax is already 4.0 for PUT from base_template

                # CALL Template
                call_template = base_template.copy()
                call_template["Name"] = f"CALL SPREAD ({time}) {plan}"
                call_template["TradeType"] = "CallSpread"
                call_template["Strategy"] = f"CALL SPREAD {plan}"
                call_template["TargetMax"] = 0.0  # For CALL, TargetMax (put target) is irrelevant
                call_template["TargetMaxCall"] = 4.0 # Default for CALL TargetMaxCall

                templates_to_process = [put_template, call_template]

                for template_data in templates_to_process:
                    cursor = conn.execute(
                        "SELECT TradeTemplateID, IsDeleted FROM TradeTemplate WHERE Name = ?",
                        (template_data["Name"],)
                    )
                    result = cursor.fetchone()
                    if result:
                        trade_template_id, is_deleted = result
                        logging.info(
                            f"TradeTemplate already exists: {template_data['Name']} with ID {trade_template_id}"
                        )
                        if is_deleted != 0:
                            conn.execute(
                                "UPDATE TradeTemplate SET IsDeleted = 0 WHERE TradeTemplateID = ?",
                                (trade_template_id,)
                            )
                            logging.info(f"Reactivated TradeTemplate: {template_data['Name']}")
                        # Optionally, update existing non-deleted templates if needed,
                        # but current logic only inserts if not found or reactivates if deleted.
                        # For a full sync of defaults, an UPDATE would be needed here.
                    else:
                        # Ensure all keys from template_data are actual columns in TradeTemplate
                        # The schema of TradeTemplate table must match keys in template_data
                        cols = ', '.join(f"[{col}]" for col in template_data.keys())
                        placeholders = ', '.join(['?'] * len(template_data))
                        vals = tuple(template_data.values())
                        try:
                            conn.execute(
                                f"INSERT INTO TradeTemplate ({cols}) VALUES ({placeholders})",
                                vals
                            )
                            logging.info(f"Inserted TradeTemplate: {template_data['Name']}")
                        except sqlite3.Error as insert_e:
                            logging.error(f"Error inserting TradeTemplate {template_data['Name']}: {insert_e}")
                            logging.error(f"Columns: {cols}")
                            logging.error(f"Values: {vals}")
                            raise # Re-raise the exception to be caught by the main try-except

        logging.info("TradeTemplates created or updated successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error processing TradeTemplates: {str(e)}")
        raise # Re-raise to be handled by the caller


def create_schedules(conn, plan_suffixes, trade_condition_ids, accounts, times, active=True):
    """
    Create or update ScheduleMaster entries for each plan/time/account.
    """
    try:
        for plan in plan_suffixes:
            for time in times:
                hour, minute = map(int, time.split(':'))

                # GET PUT template
                put_name = f"PUT SPREAD ({time}) {plan}"
                cursor = conn.execute(
                    "SELECT TradeTemplateID FROM TradeTemplate WHERE Name = ?",
                    (put_name,)
                )
                row = cursor.fetchone()
                if not row:
                    # This case should ideally not happen if create_trade_templates ran correctly
                    logging.error(f"Missing PUT template '{put_name}' during schedule creation. Skipping.")
                    continue
                put_template_id = row[0]

                # GET CALL template
                call_name = f"CALL SPREAD ({time}) {plan}"
                cursor = conn.execute(
                    "SELECT TradeTemplateID FROM TradeTemplate WHERE Name = ?",
                    (call_name,)
                )
                row = cursor.fetchone()
                if not row:
                    # This case should ideally not happen
                    logging.error(f"Missing CALL template '{call_name}' during schedule creation. Skipping.")
                    continue
                call_template_id = row[0]

                # Default conditions if specific strategy mapping isn't available/complex
                # For this script, we use a fixed mapping based on EMA strategies.
                put_strategy_key = 'EMA520'
                call_strategy_key = 'EMA520_INV'

                put_cond_data = trade_condition_ids.get(put_strategy_key)
                call_cond_data = trade_condition_ids.get(call_strategy_key)

                if not (put_cond_data and call_cond_data):
                    logging.error(f"Missing default trade conditions ({put_strategy_key} or {call_strategy_key}) for plan {plan}, time {time}. Skipping schedule creation.")
                    continue # Skip this schedule if base conditions are missing

                for account in accounts:
                    # Schedule for PUT
                    # Check if schedule already exists to avoid duplicates if script is re-run without full force-init
                    cursor = conn.execute("""
                        SELECT ScheduleMasterID FROM ScheduleMaster
                        WHERE TradeTemplateID=? AND Strategy=? AND Account=? AND Hour=? AND Minute=?
                    """, (put_template_id, put_strategy_key, account, hour, minute))
                    if not cursor.fetchone():
                        conn.execute("""
                            INSERT INTO ScheduleMaster (
                                Account, TradeTemplateID, ScheduleType, QtyOverride,
                                Hour, Minute, Second, ExpirationMinutes, IsActive,
                                ScheduleGroupID, Condition, Strategy, DisplayStrategy,
                                TradeConditionID, DisplayCondition,
                                DayMonday, DayTuesday, DayWednesday, DayThursday, DayFriday, DaySunday,
                                QtyType, QtyAllocation, QtyAllocationMax
                            )
                            VALUES (
                                ?, ?, 'Trade', 1, /* QtyOverride default, updated by CSV processing */
                                ?, ?, 0, 5, ?,    /* ExpirationMinutes=5, IsActive based on param */
                                0, NULL, ?, ?, ?, ?, /* ScheduleGroupID=0, Condition=NULL, Strategy=key, DisplayStrategy=friendly */
                                1, 1, 1, 1, 1, 0, /* Days Active */
                                'FixedQty', 0.0, 0 /* QtyType defaults */
                            )
                        """, (
                            account, put_template_id,
                            hour, minute, int(active),
                            put_strategy_key, f"PUT SPREAD {plan}", # Strategy key and display name
                            put_cond_data['id'], put_cond_data['description']
                        ))
                        logging.info(f"Inserted PUT schedule for {account}, {put_name}")
                    else:
                        logging.info(f"PUT schedule for {account}, {put_name} at {time} already exists. Skipping insertion.")


                    # Schedule for CALL
                    cursor = conn.execute("""
                        SELECT ScheduleMasterID FROM ScheduleMaster
                        WHERE TradeTemplateID=? AND Strategy=? AND Account=? AND Hour=? AND Minute=?
                    """, (call_template_id, call_strategy_key, account, hour, minute))
                    if not cursor.fetchone():
                        conn.execute("""
                            INSERT INTO ScheduleMaster (
                                Account, TradeTemplateID, ScheduleType, QtyOverride,
                                Hour, Minute, Second, ExpirationMinutes, IsActive,
                                ScheduleGroupID, Condition, Strategy, DisplayStrategy,
                                TradeConditionID, DisplayCondition,
                                DayMonday, DayTuesday, DayWednesday, DayThursday, DayFriday, DaySunday,
                                QtyType, QtyAllocation, QtyAllocationMax
                            )
                            VALUES (
                                ?, ?, 'Trade', 1,
                                ?, ?, 0, 5, ?,
                                0, NULL, ?, ?, ?, ?,
                                1, 1, 1, 1, 1, 0,
                                'FixedQty', 0.0, 0
                            )
                        """, (
                            account, call_template_id,
                            hour, minute, int(active),
                            call_strategy_key, f"CALL SPREAD {plan}", # Strategy key and display name
                            call_cond_data['id'], call_cond_data['description']
                        ))
                        logging.info(f"Inserted CALL schedule for {account}, {call_name}")
                    else:
                        logging.info(f"CALL schedule for {account}, {call_name} at {time} already exists. Skipping insertion.")


        logging.info("ScheduleMaster entries created or verified successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error inserting or verifying ScheduleMaster entries: {str(e)}")
        raise


def verify_put_update(conn, trade_template_id, expected_target_max, expected_long_width, expected_stop_multiple):
    """
    Verify that the PUT Spread Template has correct columns. (Utility/Test function)
    """
    try:
        cursor = conn.execute("""
            SELECT TargetMax, LongWidth, StopMultiple
            FROM TradeTemplate
            WHERE TradeTemplateID = ?
        """, (trade_template_id,))
        row = cursor.fetchone()
        if row:
            actual_target_max, actual_long_width, actual_stop_multiple = row
            # Handle potential None values from DB if columns can be NULL
            actual_target_max = actual_target_max if actual_target_max is not None else 0.0 # Assuming 0.0 if NULL
            actual_stop_multiple = actual_stop_multiple if actual_stop_multiple is not None else 0.0

            if (abs(actual_target_max - expected_target_max) < 0.001 # For float comparison
                and actual_long_width == expected_long_width
                and abs(actual_stop_multiple - expected_stop_multiple) < 0.001): # For float comparison
                return True
            else:
                logging.error(f"Verification FAILED for PUT TradeTemplateID {trade_template_id}. Expected: (TargetMax={expected_target_max}, LongWidth='{expected_long_width}', StopMultiple={expected_stop_multiple}), Actual: (TargetMax={actual_target_max}, LongWidth='{actual_long_width}', StopMultiple={actual_stop_multiple})")
                return False
        else:
            logging.error(f"Verification FAILED: PUT TradeTemplateID {trade_template_id} not found.")
            return False
    except sqlite3.Error as e:
        logging.error(f"Error verifying PUT for TradeTemplateID {trade_template_id}: {e}")
        return False


def verify_call_update(conn, trade_template_id, expected_target_max_call, expected_long_width, expected_stop_multiple):
    """
    Verify that the CALL Spread Template has correct columns. (Utility/Test function)
    """
    try:
        cursor = conn.execute("""
            SELECT TargetMaxCall, LongWidth, StopMultiple
            FROM TradeTemplate
            WHERE TradeTemplateID = ?
        """, (trade_template_id,))
        row = cursor.fetchone()
        if row:
            actual_tmax_call, actual_lwidth, actual_stop_mult = row
            # Handle potential None values
            actual_tmax_call = actual_tmax_call if actual_tmax_call is not None else 0.0
            actual_stop_mult = actual_stop_mult if actual_stop_mult is not None else 0.0

            if (abs(actual_tmax_call - expected_target_max_call) < 0.001
                and actual_lwidth == expected_long_width
                and abs(actual_stop_mult - expected_stop_multiple) < 0.001):
                return True
            else:
                logging.error(f"Verification FAILED for CALL TradeTemplateID {trade_template_id}. Expected: (TargetMaxCall={expected_target_max_call}, LongWidth='{expected_long_width}', StopMultiple={expected_stop_multiple}), Actual: (TargetMaxCall={actual_tmax_call}, LongWidth='{actual_lwidth}', StopMultiple={actual_stop_mult})")
                return False
        else:
            logging.error(f"Verification FAILED: CALL TradeTemplateID {trade_template_id} not found.")
            return False
    except sqlite3.Error as e:
        logging.error(f"Error verifying CALL for TradeTemplateID {trade_template_id}: {e}")
        return False


def initialize_database(conn, plan_count, force, accounts, times):
    """
    Initialize entire DB: optionally delete old data, create conditions/templates/schedules.
    """
    try:
        conn.execute("BEGIN TRANSACTION")
        if force:
            logging.info("Force-initializing: Deleting existing data from relevant tables.")
            conn.execute("DELETE FROM ScheduleMaster")
            conn.execute("DELETE FROM TradeTemplate") # Delete templates before conditions if there are FKs
            conn.execute("DELETE FROM TradeConditionDetail")
            conn.execute("DELETE FROM TradeCondition")
            logging.info("Cleared existing data from TradeTemplate, ScheduleMaster, TradeConditionDetail, TradeCondition.")
            try:
                # Reset auto-increment counters for these tables
                conn.execute("DELETE FROM sqlite_sequence WHERE name='TradeCondition'")
                conn.execute("DELETE FROM sqlite_sequence WHERE name='TradeTemplate'")
                conn.execute("DELETE FROM sqlite_sequence WHERE name='ScheduleMaster'")
                logging.info("Reset auto-increment sequences for relevant tables.")
            except sqlite3.Error as e:
                # This might fail if the table was empty and thus no entry in sqlite_sequence
                logging.warning(f"Could not reset ID sequences (this might be normal if tables were empty): {e}")

        trade_condition_ids = create_trade_conditions(conn) # Creates or verifies conditions
        plan_suffixes = [f"P{i}" for i in range(1, plan_count + 1)]
        create_trade_templates(conn, plan_suffixes, times) # Creates or verifies templates

        # Create schedules if accounts are provided, for both --initialize and --force-initialize
        if not accounts:
            logging.error("No accounts provided for initialization. Schedules will not be created.")
            print("Error: No accounts provided for initialization. Schedules will not be created.")
        else:
            # Create schedules as inactive for both init types for consistency.
            create_schedules(conn, plan_suffixes, trade_condition_ids, accounts, times, active=False)

        conn.commit()
        if force:
            print(f"Force-initialized DB with TradeConditions, TradeTemplates for P1..P{plan_count}.")
        else:
            print(f"Standard initialization complete. Ensured TradeConditions and TradeTemplates up to P{plan_count} exist.")
        
        if accounts:
            print("Schedules created (inactive) or verified.")
        else:
            print("Schedules NOT created due to missing accounts.")

    except Exception as e: # Catch any exception during initialization
        logging.error(f"Error during database initialization: {e}", exc_info=True)
        try:
            conn.rollback()
            logging.info("Rolled back database transaction due to initialization error.")
        except sqlite3.Error as rb_e:
            logging.error(f"Error during rollback: {rb_e}")
        raise # Re-raise the original exception


def update_template_profit_target(conn, template_id, profit_target_percentage):
    """
    Helper function to update profit target fields for a given template ID.
    """
    profit_target_type_val = None
    profit_target_val = None
    order_id_profit_target_val = "None"  # Default to "None"

    if profit_target_percentage is not None: # This implies it's already a float or None from process_tradeplan
        if not np.isnan(profit_target_percentage): # Check if it's a valid number
            profit_target_val = profit_target_percentage
            profit_target_type_val = "Percentage"
            order_id_profit_target_val = "ProfitTarget" # Activate profit target mechanism
            logging.info(f"Setting ProfitTarget for TradeTemplateID {template_id}: Type='Percentage', Target={profit_target_val:.2f}")
        else: # profit_target_percentage was NaN
            logging.warning(f"Profit target value was NaN for TradeTemplateID {template_id}. Clearing profit target.")
    else: # profit_target_percentage was None
        logging.info(f"Clearing ProfitTarget for TradeTemplateID {template_id}.")


    conn.execute("""
        UPDATE TradeTemplate
        SET ProfitTargetType = ?, ProfitTarget = ?, OrderIDProfitTarget = ?
        WHERE TradeTemplateID = ?
    """, (profit_target_type_val, profit_target_val, order_id_profit_target_val, template_id))


def update_put_template(conn, template_name, premium, spread, stop_multiple, profit_target_percentage):
    """
    Updates specific fields of a PUT TradeTemplate.
    """
    cursor = conn.execute(
        "SELECT TradeTemplateID FROM TradeTemplate WHERE Name = ?",
        (template_name,)
    )
    row = cursor.fetchone()
    if not row:
        # This should not happen if create_trade_templates ensures all templates exist
        logging.error(f"CRITICAL: Missing PUT template '{template_name}' during update. This indicates an issue with initialization or template creation.")
        raise ValueError(f"Missing PUT template {template_name}")

    tid = row[0]
    conn.execute("""
        UPDATE TradeTemplate
        SET TargetMax = ?, LongWidth = ?, StopMultiple = ?
        WHERE TradeTemplateID = ?
    """, (premium, spread, stop_multiple, tid))
    logging.debug(f"Updated base fields for PUT template {template_name} (ID: {tid}).")
    update_template_profit_target(conn, tid, profit_target_percentage) # Update profit target fields


def update_call_template(conn, template_name, premium, spread, stop_multiple, profit_target_percentage):
    """
    Updates specific fields of a CALL TradeTemplate.
    'premium' here refers to TargetMaxCall.
    """
    cursor = conn.execute(
        "SELECT TradeTemplateID FROM TradeTemplate WHERE Name = ?",
        (template_name,)
    )
    row = cursor.fetchone()
    if not row:
        logging.error(f"CRITICAL: Missing CALL template '{template_name}' during update. This indicates an issue with initialization or template creation.")
        raise ValueError(f"Missing CALL template {template_name}")

    tid = row[0]
    # For CALL, 'premium' from CSV likely maps to TargetMinCall or similar,
    # and TargetMaxCall might be different or also derived from 'premium'.
    # The original script set TargetMaxCall = premium.
    # StopMultiple and LongWidth are common.
    conn.execute("""
        UPDATE TradeTemplate
        SET TargetMaxCall = ?, LongWidth = ?, StopMultiple = ?
        WHERE TradeTemplateID = ?
    """, (premium, spread, stop_multiple, tid)) # Assuming premium maps to TargetMaxCall for CALLs
    logging.debug(f"Updated base fields for CALL template {template_name} (ID: {tid}).")
    update_template_profit_target(conn, tid, profit_target_percentage) # Update profit target fields


def update_schedule_master_entry(conn, template_name, qty_override, ema_strategy_key,
                                 condition_id, condition_desc, plan_suffix, option_type_str):
    """
    Updates a single ScheduleMaster entry. Creates if not exists, based on template name.
    option_type_str is "PUT" or "CALL" for logging/display purposes.
    """
    cursor = conn.execute(
        "SELECT TradeTemplateID FROM TradeTemplate WHERE Name = ?",
        (template_name,)
    )
    row = cursor.fetchone()
    if not row:
        logging.error(f"Missing {option_type_str} TradeTemplate for schedule: {template_name}")
        # Depending on strictness, could raise ValueError or just skip
        return
    trade_template_id = row[0]

    display_strategy_name = f"{option_type_str} SPREAD {plan_suffix}"

    update_sql = """
        UPDATE ScheduleMaster
        SET IsActive = 1, QtyOverride = ?, Strategy = ?,
            TradeConditionID = ?, DisplayStrategy = ?,
            DisplayCondition = ?
        WHERE TradeTemplateID = ? 
    """
    # Note: The WHERE clause `WHERE TradeTemplateID = ?` will update ALL schedules associated
    # with this template ID. If schedules are also distinguished by Account,
    # the WHERE clause would need to include `AND Account = ?`.
    # The current script design implies schedules are primarily linked via TradeTemplateID for these updates.

    params = (
        qty_override, ema_strategy_key, condition_id,
        display_strategy_name, condition_desc, trade_template_id
    )
    
    # To provide more accurate logging on updates, we can check how many rows will be affected.
    # However, this requires another query. For simplicity, we'll log the attempt.
    # If specific per-account updates are needed, the logic here and the SQL would need adjustment.
    
    # Execute the update
    update_cursor = conn.execute(update_sql, params)
    
    if update_cursor.rowcount > 0:
        logging.info(f"Activated and updated {update_cursor.rowcount} schedule(s) for {template_name} (TemplateID: {trade_template_id}) with Qty={qty_override}, Strategy='{ema_strategy_key}'.")
    else:
        logging.warning(f"No schedules found (or no changes made) for {template_name} (TemplateID: {trade_template_id}) to activate/update. This might be normal if schedules are missing or already had these values (excluding IsActive).")


def process_tradeplan(conn, data, trade_condition_ids):
    """
    Reads the DataFrame row by row, updates the relevant templates & schedules.
    """

    if "Plan" not in data.columns:
        logging.info("No 'Plan' column in CSV, defaulting all entries to 'P1'.")
        data['Plan'] = 'P1'
    else:
        data['Plan'] = data['Plan'].fillna('P1').astype(str).str.upper()
        logging.info("Normalized 'Plan' column (filled NaN with P1, converted to uppercase string).")

    has_option_type_column = ("OptionType" in data.columns)
    if not has_option_type_column:
        logging.info("No 'OptionType' column in CSV. Script will operate in legacy mode (updating both PUT and CALL templates/schedules per row).")


    # Validate required strategies from CSV against available conditions
    required_strategies_from_csv = set()
    if 'Strategy' in data.columns and not data['Strategy'].empty:
        for strategy_val in data['Strategy'].dropna().unique(): # dropna handles NaN values from numeric conversion
            s_up = str(strategy_val).upper()
            # Handle cases where strategy might have been read as float (e.g. "520.0") then string "520.0"
            if s_up.endswith(".0"): 
                s_up = s_up[:-2]
            
            if s_up in ["EMA520", "EMA540", "EMA2040"]:
                required_strategies_from_csv.add(s_up)
                required_strategies_from_csv.add(f"{s_up}_INV") # Corresponding inverse
            elif s_up == 'NAN': # Explicitly ignore 'NAN' string if it appears
                logging.warning("Found 'NAN' string in 'Strategy' column, ignoring for validation.")
            else:
                logging.error(f"Unsupported Strategy '{strategy_val}' (parsed as '{s_up}') found in CSV. Ensure strategies are one of EMA520, EMA540, EMA2040.")
                raise ValueError(f"Unsupported Strategy '{strategy_val}' in CSV.")
    else:
        logging.warning("'Strategy' column is missing or empty in CSV. Defaulting to EMA520/EMA520_INV where applicable.")
        if not ("EMA520" in trade_condition_ids and "EMA520_INV" in trade_condition_ids) :
            logging.error("Default strategies EMA520/EMA520_INV are required but not found in trade_condition_ids. Cannot proceed.")
            raise ValueError("Default trade conditions EMA520/EMA520_INV are missing.")


    missing_db_conditions = required_strategies_from_csv - set(trade_condition_ids.keys())
    if missing_db_conditions:
        logging.error(f"Missing TradeConditions in database for strategies required by CSV: {', '.join(missing_db_conditions)}")
        raise ValueError(f"Database is missing TradeConditions for: {', '.join(missing_db_conditions)}")

    # Iterate through each row in the trade plan CSV
    for idx, row in data.iterrows():
        try:
            plan_suffix = str(row.get('Plan', 'P1')) 
            hour_minute = str(row.get('Hour:Minute', '')).strip() # Ensure it's a string and stripped

            if not hour_minute: # Check for empty string after strip
                logging.warning(f"Skipping row {idx+1} due to missing or invalid 'Hour:Minute'.")
                continue

            premium_val = row.get('Premium')
            premium = float(premium_val) if pd.notna(premium_val) else 0.0
            
            spread_str = str(row.get('Spread', '')) 
            
            stop_val_str = str(row.get('Stop', '0'))
            if stop_val_str.lower().endswith('x'):
                stop_multiple = float(stop_val_str[:-1]) if stop_val_str[:-1] else 0.0
            else:
                stop_multiple = float(stop_val_str) if stop_val_str else 0.0
            
            qty_val = row.get('Qty')
            qty_override = int(qty_val) if pd.notna(qty_val) else 1


            # Profit Target from CSV (already converted to numeric/NaN in main)
            profit_target_csv_value = row.get('profittarget') # This will be float or NaN
            
            # Original conversion of NaN to None
            if pd.isna(profit_target_csv_value): 
                profit_target_csv_value = None
            # MODIFICATION: If CSV profittarget is 100 (float 100.0), treat as None (no profit target)
            elif profit_target_csv_value == 100.0:
                logging.info(f"Row {idx+1} (Time: {hour_minute}, Plan: {plan_suffix}): CSV profittarget was 100.0, overriding to None (no profit target).")
                profit_target_csv_value = None
            # else: profit_target_csv_value is some other number (e.g. 50.0) or was already None.
            
            # Determine EMA strategy and corresponding condition IDs
            csv_ema_strat_val = row.get('Strategy', 'EMA520') # Default to EMA520 if missing
            csv_ema_strat = str(csv_ema_strat_val).upper()
            if csv_ema_strat.endswith(".0"): # Handle if it was like "520.0"
                csv_ema_strat = csv_ema_strat[:-2]

            if csv_ema_strat == 'NAN': 
                csv_ema_strat = 'EMA520'
                logging.warning(f"Row {idx+1}: Strategy was 'NAN', defaulted to EMA520.")

            if csv_ema_strat not in ["EMA520", "EMA540", "EMA2040"]:
                logging.warning(f"Row {idx+1}: Unsupported strategy '{csv_ema_strat}', defaulting to EMA520.")
                csv_ema_strat = 'EMA520' 

            current_put_strat_key = csv_ema_strat
            current_call_strat_key = f"{csv_ema_strat}_INV"

            cond_id_put = trade_condition_ids[current_put_strat_key]['id']
            cond_desc_put = trade_condition_ids[current_put_strat_key]['description']
            cond_id_call = trade_condition_ids[current_call_strat_key]['id']
            cond_desc_call = trade_condition_ids[current_call_strat_key]['description']

            option_types_to_process = []
            if has_option_type_column:
                opt_type_csv = str(row.get('OptionType', '')).upper().strip()
                if opt_type_csv == 'P':
                    option_types_to_process.append('PUT')
                elif opt_type_csv == 'C':
                    option_types_to_process.append('CALL')
                elif opt_type_csv == '': 
                    logging.warning(f"Row {idx+1}: 'OptionType' is empty. Processing both PUT and CALL.")
                    option_types_to_process.extend(['PUT', 'CALL'])
                else:
                    logging.error(f"Row {idx+1}: Invalid 'OptionType' '{opt_type_csv}'. Skipping.")
                    continue 
            else: 
                option_types_to_process.extend(['PUT', 'CALL'])

            for opt_to_proc in option_types_to_process:
                if opt_to_proc == 'PUT':
                    template_name = f"PUT SPREAD ({hour_minute}) {plan_suffix}"
                    update_put_template(conn, template_name, premium, spread_str, stop_multiple, profit_target_csv_value)
                    update_schedule_master_entry(conn, template_name, qty_override, current_put_strat_key,
                                                 cond_id_put, cond_desc_put, plan_suffix, "PUT")
                elif opt_to_proc == 'CALL':
                    template_name = f"CALL SPREAD ({hour_minute}) {plan_suffix}"
                    update_call_template(conn, template_name, premium, spread_str, stop_multiple, profit_target_csv_value)
                    update_schedule_master_entry(conn, template_name, qty_override, current_call_strat_key,
                                                 cond_id_call, cond_desc_call, plan_suffix, "CALL")
            
            logging.info(f"Successfully processed row {idx+1}: Plan={plan_suffix}, Time={hour_minute}, Options={option_types_to_process}, ProfitTarget={profit_target_csv_value if profit_target_csv_value is not None else 'N/A'}")

        except Exception as e_row:
            logging.error(f"Error processing CSV row {idx+1} (Data: {row.to_dict()}): {e_row}", exc_info=True)

    logging.info("Finished processing all rows in tradeplan CSV.")


def get_schedule_times():
    """
    Returns a list of trade entry times as strings in HH:MM format.
    """
    original_times_tuples = [
        (9,33), (9,39), (9,45), (9,52),
        (10,0), (10,8), (10,15), (10,23), (10,30), (10,38), (10,45), (10,53),
        (11,0), (11,8), (11,15), (11,23), (11,30), (11,38), (11,45), (11,53),
        (12,0), (12,8), (12,15), (12,23), (12,30), (12,38), (12,45), (12,53),
        (13,0), (13,8), (13,15), (13,23), (13,30), (13,38), (13,45), (13,53),
        (14,0), (14,8), (14,15), (14,23), (14,30), (14,38), (14,45), (14,53),
        (15,0), (15,8), (15,15), (15,23), (15,30), (15,38), (15,45)
    ]
    # Format the times as HH:MM strings
    formatted_times = [f"{h:02d}:{m:02d}" for h, m in original_times_tuples]
    return formatted_times


def main():
    args = parse_arguments()
    setup_logging() 

    logging.info("Script execution started.")
    logging.info(f"Arguments: {args}")


    db_path = os.path.abspath('data.db3')
    csv_path = os.path.abspath('tradeplan.csv')
    backup_dir = os.path.abspath('tradeplan-backup')

    if not os.path.exists(backup_dir):
        try:
            os.makedirs(backup_dir)
            logging.info(f"Created backup directory at: {backup_dir}")
            print(f"Created backup directory at: {backup_dir}")
        except OSError as e:
            logging.error(f"Unable to create backup directory '{backup_dir}'. Error: {str(e)}")
            print(f"Error: Unable to create backup directory '{backup_dir}'. {str(e)}")
            sys.exit(1)

    backup_filepath = create_backup(db_path, backup_dir)
    if not backup_filepath: 
        logging.error("Failed to create database backup. Exiting.")
        sys.exit(1)


    conn = None 
    data = pd.DataFrame() # Initialize data to an empty DataFrame for the final print
    try:
        conn = connect_database(db_path)

        # Get the list of times from the new function
        times = get_schedule_times()
        logging.info(f"Using entry times: {times}")


        if args.force_initialize is not None:
            plan_count = args.force_initialize
            if plan_count == -1: 
                try:
                    plan_count_input = input("Enter the number of plans (e.g., P1 to P<n>, enter n): ").strip()
                    plan_count = int(plan_count_input)
                    if plan_count < 1:
                        raise ValueError("Plan count must be a positive integer.")
                except ValueError as ve:
                    print(f"Invalid input for plan count: {ve}")
                    logging.error(f"Invalid input for plan count: {ve}")
                    sys.exit(1)
            
            print("Force-initializing database. This will delete existing templates and schedules.")
            accounts_for_init = get_accounts() 
            initialize_database(conn, plan_count, force=True, accounts=accounts_for_init, times=times)
            logging.info(f"Database force-initialized with {plan_count} plans.")
            sys.exit(0) 

        if args.initialize:
            cursor = conn.execute("SELECT Name FROM TradeTemplate WHERE Name LIKE 'PUT SPREAD (%) P%' OR Name LIKE 'CALL SPREAD (%) P%'")
            max_p = 0
            for row_name in cursor.fetchall():
                match = re.search(r'P(\d+)$', row_name[0])
                if match:
                    max_p = max(max_p, int(match.group(1)))
            plan_count_for_init = max(1, max_p) 
            
            print("Initializing database: Ensuring all required entries exist.")
            accounts_for_init = get_accounts()
            initialize_database(conn, plan_count_for_init, force=False, accounts=accounts_for_init, times=times)
            logging.info(f"Database initialized/verified for up to P{plan_count_for_init} plans.")
            sys.exit(0) 

        # --- REGULAR CSV PROCESSING ---
        logging.info(f"Loading trade plan from CSV: {csv_path}")
        try:
            data = pd.read_csv(csv_path, delimiter=',', quotechar='"', dtype=str, skipinitialspace=True)
            data = data.rename(columns=lambda x: x.strip()) 
            
            # VERBOSE OUTPUT: Loaded CSV
            print(f"Loaded tradeplan.csv with {len(data)} entries.")
            logging.info(f"Successfully loaded {len(data)} entries from {csv_path}.")


            numeric_cols = ['Premium', 'Qty', 'PnL Rank', 'profittarget'] 
            for col in numeric_cols:
                if col in data.columns:
                    data[col] = pd.to_numeric(data[col], errors='coerce')
                else:
                    if col in ['Premium', 'Qty']: 
                        logging.warning(f"Essential numeric column '{col}' not found in CSV. Defaulting or errors might occur.")
                        data[col] = np.nan 
            
        except FileNotFoundError:
            logging.error(f"CSV file not found at {csv_path}.")
            print(f"Error: CSV file not found at {csv_path}.")
            sys.exit(1)
        except pd.errors.EmptyDataError:
            logging.error(f"CSV file {csv_path} is empty.")
            print(f"Error: CSV file {csv_path} is empty.")
            sys.exit(1)
        except Exception as e_csv: 
            logging.error(f"Error reading or parsing CSV {csv_path}: {e_csv}", exc_info=True)
            print(f"Error reading or parsing CSV {csv_path}: {e_csv}")
            sys.exit(1)

        # Qty processing
        qty_source_message = "Applied default Qty=1." # Default message
        if args.qty is not None: 
            data['Qty'] = args.qty
            qty_source_message = f"Set Qty for all entries to {args.qty} (from command line)."
            logging.info(f"Overriding 'Qty' for all entries with command-line value: {args.qty}.")
        elif 'Qty' not in data.columns or data['Qty'].isna().all():
            data['Qty'] = 1 
            qty_source_message = "Applied default Qty=1 as 'Qty' column was missing or all NaN."
            logging.info(qty_source_message)
        else:
            # Qty column exists and has some non-NaN values
            data['Qty'] = data['Qty'].fillna(1) 
            qty_source_message = "Using existing 'Qty' column from CSV." # VERBOSE OUTPUT
            logging.info("Filled NaN values in 'Qty' column with 1. Using existing 'Qty' column.")
        data['Qty'] = data['Qty'].astype(int) 
        print(qty_source_message) # VERBOSE OUTPUT


        if args.distribution:
            if 'PnL Rank' not in data.columns:
                logging.error("Missing 'PnL Rank' column, cannot perform distribution.")
                print("Error: 'PnL Rank' column is required for --distribution. Skipping distribution.")
            else:
                data = data.sort_values('PnL Rank', na_position='last') 
                data['Qty'] = data['Qty'].astype(int) 
                
                top_n = 3
                if len(data) >= top_n:
                    data.iloc[:top_n, data.columns.get_loc('Qty')] += 1
                    logging.info(f"Distribution: Added 1 to Qty for top {top_n} PnL ranks.")
                
                if len(data) >= 10: 
                    data.iloc[7:10, data.columns.get_loc('Qty')] -= 1
                    logging.info("Distribution: Subtracted 1 from Qty for PnL ranks 8-10.")
                
                data['Qty'] = data['Qty'].clip(lower=0) 
                logging.info("Distribution: Ensured Qty is non-negative.")
                print("Applied PnL Rank based Qty distribution.")

        if 'Spread' in data.columns:
            data['Spread'] = data['Spread'].astype(str).str.replace('-', ',', regex=False).fillna('')
            # VERBOSE OUTPUT: Replaced dashes
            print("Replaced dashes in 'Spread' column.")
            logging.info("Processed 'Spread' column: replaced '-' with ','.")
        else:
            logging.warning("'Spread' column not found. Skipping replacement.")
        
        # Save the processed DataFrame back to tradeplan.csv
        try:
            # Select only relevant columns for saving, matching the desired final output
            columns_to_save = ['Hour:Minute', 'Premium', 'Spread', 'Stop', 'Strategy', 'Plan', 'Qty', 'profittarget', 'OptionType']
            # Filter to existing columns in the DataFrame to avoid errors if some are optional and not present
            existing_columns_to_save = [col for col in columns_to_save if col in data.columns]
            data.to_csv(csv_path, index=False, columns=existing_columns_to_save)
            # VERBOSE OUTPUT: Updated CSV saved
            print("Updated tradeplan.csv saved.")
            logging.info(f"Processed data saved back to {csv_path}")
        except Exception as e_save_csv:
            logging.error(f"Error saving processed data to CSV {csv_path}: {e_save_csv}", exc_info=True)
            print(f"Error saving updated CSV: {e_save_csv}")
            # Not necessarily a fatal error for DB update, so continue


        conn.execute("BEGIN TRANSACTION") 

        conn.execute("UPDATE ScheduleMaster SET IsActive=0")
        # VERBOSE OUTPUT: All schedules deactivated
        print("All schedules deactivated.")
        logging.info("Deactivated all entries in ScheduleMaster.")

        trade_condition_ids = create_trade_conditions(conn)
        logging.info("Trade conditions verified/created.")

        process_tradeplan(conn, data, trade_condition_ids)

        conn.commit() 
        # VERBOSE OUTPUT: TradeTemplates and Schedules updated successfully
        print("TradeTemplates and Schedules updated successfully.")
        logging.info("Successfully processed tradeplan.csv and updated database. Transaction committed.")


    except Exception as e:
        logging.error(f"An error occurred during main execution: {e}", exc_info=True)
        print(f"An error occurred: {e}")
        if conn: 
            try:
                conn.rollback()
                logging.info("Transaction rolled back due to error.")
            except sqlite3.Error as rb_err:
                logging.error(f"Error during transaction rollback: {rb_err}")
        sys.exit(1) 
    finally:
        if conn:
            try:
                conn.close()
                logging.info("Database connection closed.")
            except sqlite3.Error as e_close:
                logging.error(f"Error closing database connection: {e_close}")
        
        if backup_filepath and os.path.exists(backup_filepath):
            try:
                os.remove(backup_filepath)
                # VERBOSE OUTPUT: Removed unzipped backup
                print(f"Removed unzipped backup file: {backup_filepath}")
                logging.info(f"Removed unzipped backup file: {backup_filepath}")
            except OSError as e_remove:
                logging.warning(f"Could not remove unzipped backup file '{backup_filepath}': {e_remove}")
                print(f"Warning: Could not remove unzipped backup file '{backup_filepath}': {e_remove}")
        
        # VERBOSE OUTPUT: Script executed successfully
        print("Script executed successfully.")
        logging.info("Script execution finished.")
        
        # VERBOSE OUTPUT: Final tradeplan.csv
        print("\nFinal tradeplan.csv:")
        if not data.empty:
            # Define the columns in the desired order for the final print
            final_print_columns = ['Hour:Minute', 'Premium', 'Spread', 'Stop', 'Strategy', 'Plan', 'Qty']
            if 'profittarget' in data.columns: # Add profittarget if it exists
                final_print_columns.append('profittarget')
            if 'OptionType' in data.columns: # Add OptionType if it exists
                final_print_columns.append('OptionType')
            
            # Filter to existing columns to avoid KeyError if some are not in 'data'
            existing_final_columns = [col for col in final_print_columns if col in data.columns]
            print(data.to_csv(index=False, columns=existing_final_columns).strip())
        else:
            print("(No data to display - CSV might have been empty or loading failed before DataFrame population)")


if __name__ == "__main__":
    main()
