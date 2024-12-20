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

    Args:
        db_path (str): Path to the SQLite database file.
        backup_dir_path (str): Path to the backup directory.

    Returns:
        str: Path to the unzipped backup file.
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

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        sqlite3.Connection: SQLite database connection object.
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

    Returns:
        list: List of formatted account IDs.
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
            r'^IB:U\d{7,8}$',
            r'^IB:\d{7,8}$',
            r'^U\d{7,8}$',
            r'^\d{7,8}$'
        ]

        matched = any(re.match(pattern, account_input) for pattern in patterns)

        if not matched:
            print(
                f"Invalid format for Account ID. Please enter in the format 'IB:U########' "
                f"(e.g., {example_account})."
            )
            continue

        if account_input.startswith("IB:U") and len(account_input) in [11, 12]:
            formatted_account = account_input
        elif account_input.startswith("IB:") and len(account_input) in [10, 11]:
            formatted_account = f"IB:U{account_input[4:]}"
        elif account_input.startswith("U") and len(account_input) in [8, 9]:
            formatted_account = f"IB:{account_input}"
        elif len(account_input) in [7, 8] and account_input.isdigit():
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
    Also, set RetryUntilExpiration to 0 for all TradeConditions.

    Args:
        conn (sqlite3.Connection): SQLite database connection object.

    Returns:
        dict: Mapping of condition names to their IDs and descriptions.
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

        conn.execute("UPDATE TradeCondition SET RetryUntilExpiration = 0")
        logging.info("Set 'RetryUntilExpiration' to 0 for all TradeConditions.")

        conn.execute("UPDATE TradeConditionDetail SET ComparisonType = 'Input'")
        logging.info("Set 'ComparisonType' to 'Input' for all TradeConditionDetail entries.")

        logging.info("All default EMA conditions processed successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error inserting or updating TradeConditions: {str(e)}")
        raise

    return trade_condition_ids


def create_trade_templates(conn, plan_suffixes, times):
    """
    Create TradeTemplate entries for PUT and CALL spreads for each plan and time.
    Only inserts templates that do not already exist and ensures IsDeleted is set to 0.
    """
    try:
        for plan in plan_suffixes:
            for time in times:
                put_template = {
                    "Name": f"PUT SPREAD ({time}) {plan}",
                    "TradeType": "PutSpread",
                    "TargetType": "Credit",
                    "TargetMin": 0,
                    "TargetMax": 4.0,
                    "LongType": "Width",
                    "LongWidth": "20,25,30",
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
                    "TargetMinCall": 0,
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

                call_template = put_template.copy()
                call_template["Name"] = f"CALL SPREAD ({time}) {plan}"
                call_template["TradeType"] = "CallSpread"
                call_template["Strategy"] = f"CALL SPREAD {plan}"

                # Check PUT
                cursor = conn.execute(
                    "SELECT TradeTemplateID, IsDeleted FROM TradeTemplate WHERE Name = ?",
                    (put_template["Name"],)
                )
                result = cursor.fetchone()
                if result:
                    trade_template_id, is_deleted = result
                    logging.info(
                        f"TradeTemplate already exists: {put_template['Name']} with ID {trade_template_id}"
                    )
                    if is_deleted != 0:
                        conn.execute(
                            "UPDATE TradeTemplate SET IsDeleted = 0 WHERE TradeTemplateID = ?",
                            (trade_template_id,)
                        )
                        logging.info(
                            f"Updated 'IsDeleted' to 0 for TradeTemplate ID {trade_template_id}: "
                            f"{put_template['Name']}"
                        )
                else:
                    columns = ', '.join(put_template.keys())
                    placeholders = ', '.join(['?'] * len(put_template))
                    values = tuple(put_template.values())
                    conn.execute(
                        f"INSERT INTO TradeTemplate ({columns}) VALUES ({placeholders})",
                        values
                    )
                    logging.info(f"Inserted TradeTemplate: {put_template['Name']}")

                # Check CALL
                cursor = conn.execute(
                    "SELECT TradeTemplateID, IsDeleted FROM TradeTemplate WHERE Name = ?",
                    (call_template["Name"],)
                )
                result = cursor.fetchone()
                if result:
                    trade_template_id, is_deleted = result
                    logging.info(
                        f"TradeTemplate already exists: {call_template['Name']} with ID {trade_template_id}"
                    )
                    if is_deleted != 0:
                        conn.execute(
                            "UPDATE TradeTemplate SET IsDeleted = 0 WHERE TradeTemplateID = ?",
                            (trade_template_id,)
                        )
                        logging.info(
                            f"Updated 'IsDeleted' to 0 for TradeTemplate ID {trade_template_id}: "
                            f"{call_template['Name']}"
                        )
                else:
                    columns = ', '.join(call_template.keys())
                    placeholders = ', '.join(['?'] * len(call_template))
                    values = tuple(call_template.values())
                    conn.execute(
                        f"INSERT INTO TradeTemplate ({columns}) VALUES ({placeholders})",
                        values
                    )
                    logging.info(f"Inserted TradeTemplate: {call_template['Name']}")

        logging.info("TradeTemplates created or updated successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error inserting TradeTemplates: {str(e)}")
        raise


def create_schedules(conn, plan_suffixes, trade_condition_ids, accounts, times, active=True):
    """
    Create ScheduleMaster entries linked to TradeTemplates with appropriate EMA conditions.
    Only inserts schedules that do not already exist.
    Each account will have its own ScheduleMaster for each TradeTemplate.
    """
    try:
        for plan in plan_suffixes:
            for time in times:
                hour, minute = map(int, time.split(':'))

                put_template_name = f"PUT SPREAD ({time}) {plan}"
                cursor = conn.execute(
                    "SELECT TradeTemplateID FROM TradeTemplate WHERE Name = ?",
                    (put_template_name,)
                )
                result = cursor.fetchone()
                if result:
                    put_template_id = result[0]
                else:
                    logging.error(
                        f"PUT SPREAD TradeTemplate '{put_template_name}' not found."
                    )
                    raise ValueError(f"PUT SPREAD '{put_template_name}' not found.")

                call_template_name = f"CALL SPREAD ({time}) {plan}"
                cursor = conn.execute(
                    "SELECT TradeTemplateID FROM TradeTemplate WHERE Name = ?",
                    (call_template_name,)
                )
                result = cursor.fetchone()
                if result:
                    call_template_id = result[0]
                else:
                    logging.error(
                        f"CALL SPREAD TradeTemplate '{call_template_name}' not found."
                    )
                    raise ValueError(f"CALL SPREAD '{call_template_name}' not found.")

                for account in accounts:
                    # Using default strategies here as placeholders
                    put_strategy = 'EMA520'
                    put_trade_condition = trade_condition_ids.get(put_strategy)
                    if not put_trade_condition:
                        raise ValueError(f"TradeCondition '{put_strategy}' not found.")
                    display_strategy_put = f"PUT SPREAD {plan}"
                    display_condition_put = put_trade_condition["description"]

                    call_strategy = 'EMA520_INV'
                    call_trade_condition = trade_condition_ids.get(call_strategy)
                    if not call_trade_condition:
                        raise ValueError(f"TradeCondition '{call_strategy}' not found.")
                    display_strategy_call = f"CALL SPREAD {plan}"
                    display_condition_call = call_trade_condition["description"]

                    # PUT Schedule
                    cursor = conn.execute("""
                        SELECT ScheduleMasterID FROM ScheduleMaster
                        WHERE TradeTemplateID = ? AND Strategy = ? AND Account = ?
                    """, (put_template_id, put_strategy, account))
                    if not cursor.fetchone():
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
                            account, put_template_id, "Trade", 1,
                            hour, minute, 0, 5, int(active),
                            0, None, put_strategy, display_strategy_put,
                            put_trade_condition["id"], display_condition_put,
                            1, 1, 1, 1, 1, 0,
                            "FixedQty", 0.0, 0
                        ))
                        status = "active" if active else "inactive"
                        logging.info(
                            f"Inserted ScheduleMaster for PUT SPREAD '{put_template_name}' "
                            f"with Strategy {put_strategy} and Account '{account}' as {status}."
                        )

                    # CALL Schedule
                    cursor = conn.execute("""
                        SELECT ScheduleMasterID FROM ScheduleMaster
                        WHERE TradeTemplateID = ? AND Strategy = ? AND Account = ?
                    """, (call_template_id, call_strategy, account))
                    if not cursor.fetchone():
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
                            account, call_template_id, "Trade", 1,
                            hour, minute, 0, 5, int(active),
                            0, None, call_strategy, display_strategy_call,
                            call_trade_condition["id"], display_condition_call,
                            1, 1, 1, 1, 1, 0,
                            "FixedQty", 0.0, 0
                        ))
                        status = "active" if active else "inactive"
                        logging.info(
                            f"Inserted ScheduleMaster for CALL SPREAD '{call_template_name}' "
                            f"with Strategy {call_strategy} and Account '{account}' as {status}."
                        )

        logging.info("ScheduleMaster entries created or updated successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error inserting ScheduleMaster entries: {str(e)}")
        raise


def verify_put_update(conn, trade_template_id, expected_target_max, expected_long_width, expected_stop_multiple):
    """
    Verify that the PUT Spread TradeTemplate has been updated correctly.
    """
    try:
        cursor = conn.execute("""
            SELECT TargetMax, LongWidth, StopMultiple FROM TradeTemplate
            WHERE TradeTemplateID = ?
        """, (trade_template_id,))
        result = cursor.fetchone()
        if result:
            actual_target_max, actual_long_width, actual_stop_multiple = result
            if (actual_target_max == expected_target_max and
                actual_long_width == expected_long_width and
                actual_stop_multiple == expected_stop_multiple):
                logging.info(f"Verification SUCCESS for PUT TradeTemplateID {trade_template_id}.")
                return True
            else:
                logging.error(f"Verification FAILED for PUT TradeTemplateID {trade_template_id}:")
                logging.error(f"  Expected TargetMax={expected_target_max}, Got {actual_target_max}")
                logging.error(f"  Expected LongWidth='{expected_long_width}', Got '{actual_long_width}'")
                logging.error(f"  Expected StopMultiple={expected_stop_multiple}, Got {actual_stop_multiple}")
                return False
        else:
            logging.error(f"Verification FAILED: TradeTemplateID {trade_template_id} not found.")
            return False
    except sqlite3.Error as e:
        logging.error(f"Error during PUT verification: {str(e)}")
        return False


def verify_call_update(conn, trade_template_id, expected_target_max_call, expected_long_width, expected_stop_multiple):
    """
    Verify that the CALL Spread TradeTemplate has been updated correctly.
    """
    try:
        cursor = conn.execute("""
            SELECT TargetMaxCall, LongWidth, StopMultiple FROM TradeTemplate
            WHERE TradeTemplateID = ?
        """, (trade_template_id,))
        result = cursor.fetchone()
        if result:
            actual_target_max_call, actual_long_width, actual_stop_multiple = result
            if (actual_target_max_call == expected_target_max_call and
                actual_long_width == expected_long_width and
                actual_stop_multiple == expected_stop_multiple):
                logging.info(f"Verification SUCCESS for CALL TradeTemplateID {trade_template_id}.")
                return True
            else:
                logging.error(f"Verification FAILED for CALL TradeTemplateID {trade_template_id}:")
                logging.error(f"  Expected TargetMaxCall={expected_target_max_call}, Got {actual_target_max_call}")
                logging.error(f"  Expected LongWidth='{expected_long_width}', Got '{actual_long_width}'")
                logging.error(f"  Expected StopMultiple={expected_stop_multiple}, Got {actual_stop_multiple}")
                return False
        else:
            logging.error(f"Verification FAILED: TradeTemplateID {trade_template_id} not found.")
            return False
    except sqlite3.Error as e:
        logging.error(f"Error during CALL verification: {str(e)}")
        return False


def initialize_database(conn, plan_count, force, accounts, times):
    """
    Initialize the database with TradeConditions, TradeTemplates, and ScheduleMaster entries.
    If force is True, it deletes existing entries and creates schedules as inactive.
    Otherwise, it inserts missing entries without deleting.
    """
    conn.execute("BEGIN TRANSACTION")
    try:
        if force:
            conn.execute("DELETE FROM TradeTemplate")
            conn.execute("DELETE FROM ScheduleMaster")
            conn.execute("DELETE FROM TradeConditionDetail")
            conn.execute("DELETE FROM TradeCondition")
            logging.info("Cleared existing TradeTemplates, Schedules, and TradeConditions.")

            try:
                conn.execute("DELETE FROM sqlite_sequence WHERE name='TradeCondition'")
                logging.info("Reset TradeCondition ID sequence.")
            except sqlite3.Error as e:
                logging.warning(
                    f"Could not reset TradeCondition ID sequence. {str(e)}"
                )

        trade_condition_ids = create_trade_conditions(conn)
        plan_suffixes = [f"P{i}" for i in range(1, plan_count + 1)]
        create_trade_templates(conn, plan_suffixes, times)

        if force:
            if not accounts:
                logging.error("No accounts provided for force-initialize.")
                print("Error: No accounts provided for force-initialize.")
                conn.rollback()
                conn.close()
                sys.exit(1)
            create_schedules(
                conn,
                plan_suffixes,
                trade_condition_ids,
                accounts,
                times,
                active=False
            )

        conn.commit()
        if force:
            logging.info(
                f"Initialized database with TradeTemplates for plans P1 to P{plan_count}, "
                f"and associated Schedules as inactive."
            )
            print(
                f"Initialized database with TradeTemplates for plans P1 to P{plan_count}, "
                f"and associated Schedules as inactive."
            )
        else:
            logging.info("Inserted missing TradeTemplates and ScheduleMaster entries.")
            print("Inserted missing TradeTemplates and ScheduleMaster entries.")

    except sqlite3.Error as e:
        logging.error(f"Error occurred during initialization: {str(e)}")
        conn.rollback()
        conn.close()
        sys.exit(1)
    except ValueError as ve:
        logging.error(str(ve))
        conn.rollback()
        conn.close()
        sys.exit(1)


def process_tradeplan(conn, data, trade_condition_ids):
    """
    Process the tradeplan.csv and update TradeTemplates and ScheduleMaster accordingly.
    """
    # Determine if "Plan" column exists
    if "Plan" in data.columns:
        data['Plan'] = data['Plan'].str.upper()
        unique_plans = data['Plan'].unique().tolist()
        logging.info(f"Processing plans: {', '.join(unique_plans)}")
    else:
        data['Plan'] = 'P1'
        unique_plans = ['P1']
        logging.info("No 'Plan' column found in the tradeplan.csv. Treating all entries as Plan P1.")

    required_conditions = set()
    for strategy in data['Strategy'].unique():
        strategy_upper = strategy.upper()
        if strategy_upper in ["EMA520", "EMA540", "EMA2040"]:
            required_conditions.add(strategy_upper)
            required_conditions.add(f"{strategy_upper}_INV")
        else:
            logging.error(f"Unsupported Strategy '{strategy}' found in CSV.")
            print(f"Error: Unsupported Strategy '{strategy}' found in CSV.")
            conn.close()
            sys.exit(1)

    missing_conditions = required_conditions - set(trade_condition_ids.keys())
    if missing_conditions:
        missing_str = ', '.join(missing_conditions)
        logging.warning(f"Missing TradeConditionIDs for strategies: {missing_str}")
        print(f"\nWARNING: Missing TradeConditionIDs for strategies: {missing_str}")
        print(
            "Please run the script with '--force-initialize' to create the necessary TradeConditions."
        )
        conn.close()
        sys.exit(1)

    required_trade_templates = set()
    for _, row in data.iterrows():
        plan = row['Plan']
        time = row['Hour:Minute']
        required_trade_templates.add(f"PUT SPREAD ({time}) {plan}")
        required_trade_templates.add(f"CALL SPREAD ({time}) {plan}")

    cursor = conn.execute("SELECT Name FROM TradeTemplate")
    existing_trade_templates = set(name for (name,) in cursor.fetchall())

    missing_trade_templates = required_trade_templates - existing_trade_templates
    if missing_trade_templates:
        missing_templates_str = ', '.join(missing_trade_templates)
        logging.warning(f"Missing TradeTemplates: {missing_templates_str}")
        print(f"\nWARNING: Missing TradeTemplates: {missing_templates_str}")
        print(
            "Please run the script with '--force-initialize' to create the necessary TradeTemplates."
        )
        conn.close()
        sys.exit(1)

    # Start a transaction for processing the plan
    conn.execute("BEGIN TRANSACTION")
    try:
        for index, row in data.iterrows():
            plan = row['Plan']
            hour_minute = row['Hour:Minute']

            if 'Premium' not in row or pd.isna(row['Premium']):
                logging.error(
                    f"Missing 'Premium' value at row {index + 1}."
                )
                print(
                    f"Error: Missing 'Premium' value at row {index + 1}."
                )
                raise ValueError("Missing Premium")

            premium = float(row['Premium'])
            spread = row['Spread'].replace('-', ',') if 'Spread' in data.columns else "20,25,30"
            stop_str = str(row['Stop'])
            if stop_str.endswith(('x', 'X')):
                stop_multiple = float(stop_str[:-1])
            else:
                stop_multiple = float(stop_str)

            qty_override = int(row['Qty'])
            ema_strategy = row['Strategy'].upper()

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
                logging.error(
                    f"Unknown Strategy '{ema_strategy}' at row {index + 1}."
                )
                print(
                    f"Error: Unknown Strategy '{ema_strategy}' at row {index + 1}."
                )
                raise ValueError("Unknown Strategy")

            put_template_name = f"PUT SPREAD ({hour_minute}) {plan}"
            cursor = conn.execute(
                "SELECT TradeTemplateID FROM TradeTemplate WHERE Name = ?",
                (put_template_name,)
            )
            result = cursor.fetchone()
            if result:
                put_template_id = result[0]
                conn.execute("""
                    UPDATE TradeTemplate
                    SET TargetMax = ?, LongWidth = ?, StopMultiple = ?
                    WHERE TradeTemplateID = ?
                """, (premium, spread, stop_multiple, put_template_id))
                logging.info(f"Updated TradeTemplate for PUT SPREAD '{put_template_name}'.")
                print(f"Updated TradeTemplate for PUT SPREAD '{put_template_name}'.")

                # Verification
                verification_passed = verify_put_update(
                    conn,
                    put_template_id,
                    expected_target_max=premium,
                    expected_long_width=spread,
                    expected_stop_multiple=stop_multiple
                )
                if verification_passed:
                    print(f"Verification SUCCESS for PUT SPREAD '{put_template_name}'.")
                else:
                    raise ValueError(f"Verification FAILED for PUT SPREAD '{put_template_name}'.")

                conn.execute("""
                    UPDATE ScheduleMaster
                    SET IsActive = 1, QtyOverride = ?, Strategy = ?, 
                        TradeConditionID = ?, DisplayStrategy = ?, 
                        DisplayCondition = ?
                    WHERE TradeTemplateID = ?
                """, (
                    qty_override, ema_strategy, condition_id_put,
                    f"PUT SPREAD {plan}", condition_name_put, put_template_id
                ))
                logging.info(f"Updated ScheduleMaster for PUT SPREAD '{put_template_name}'.")
                print(f"Updated ScheduleMaster for PUT SPREAD '{put_template_name}'.")

            else:
                logging.warning(
                    f"PUT SPREAD TradeTemplate '{put_template_name}' not found."
                )
                raise ValueError(f"PUT SPREAD '{put_template_name}' not found.")

            call_template_name = f"CALL SPREAD ({hour_minute}) {plan}"
            cursor = conn.execute(
                "SELECT TradeTemplateID FROM TradeTemplate WHERE Name = ?",
                (call_template_name,)
            )
            result = cursor.fetchone()
            if result:
                call_template_id = result[0]
                conn.execute("""
                    UPDATE TradeTemplate
                    SET TargetMaxCall = ?, LongWidth = ?, StopMultiple = ?
                    WHERE TradeTemplateID = ?
                """, (premium, spread, stop_multiple, call_template_id))
                logging.info(f"Updated TradeTemplate for CALL SPREAD '{call_template_name}'.")
                print(f"Updated TradeTemplate for CALL SPREAD '{call_template_name}'.")

                # Verification
                verification_passed = verify_call_update(
                    conn,
                    call_template_id,
                    expected_target_max_call=premium,
                    expected_long_width=spread,
                    expected_stop_multiple=stop_multiple
                )
                if verification_passed:
                    print(f"Verification SUCCESS for CALL SPREAD '{call_template_name}'.")
                else:
                    raise ValueError(f"Verification FAILED for CALL SPREAD '{call_template_name}'.")

                conn.execute("""
                    UPDATE ScheduleMaster
                    SET IsActive = 1, QtyOverride = ?, Strategy = ?, 
                        TradeConditionID = ?, DisplayStrategy = ?, 
                        DisplayCondition = ?
                    WHERE TradeTemplateID = ?
                """, (
                    qty_override, f"{ema_strategy}_INV", condition_id_call,
                    f"CALL SPREAD {plan}", condition_name_call, call_template_id
                ))
                logging.info(f"Updated ScheduleMaster for CALL SPREAD '{call_template_name}'.")
                print(f"Updated ScheduleMaster for CALL SPREAD '{call_template_name}'.")
            else:
                logging.warning(
                    f"CALL SPREAD TradeTemplate '{call_template_name}' not found."
                )
                raise ValueError(f"CALL SPREAD '{call_template_name}' not found.")

        conn.commit()
        logging.info("\nTradeTemplates and ScheduleMaster entries updated successfully.")
        print("\nTradeTemplates and ScheduleMaster entries updated successfully.")

    except Exception as e:
        logging.error(f"Error occurred while processing tradeplan: {str(e)}")
        conn.rollback()
        raise


def main():
    """
    Main function to execute the script logic.
    """
    args = parse_arguments()
    setup_logging()

    # Paths
    db_path = 'data.db3'
    db_path = os.path.abspath(db_path)
    csv_path = 'tradeplan.csv'

    backup_dir = 'tradeplan-backup'
    backup_dir_path = os.path.abspath(backup_dir)
    if not os.path.exists(backup_dir_path):
        try:
            os.makedirs(backup_dir_path)
            logging.info(f"Created backup directory at: {backup_dir_path}")
            print(f"Created backup directory at: {backup_dir_path}")
        except OSError as e:
            logging.error(f"Unable to create backup directory. {str(e)}")
            print(f"Error: Unable to create backup directory. {str(e)}")
            sys.exit(1)

    backup_filepath = create_backup(db_path, backup_dir_path)
    conn = connect_database(db_path)

    times = [
        "09:33", "09:45", "10:00", "10:15", "10:30", "10:45", "11:00",
        "11:15", "11:30", "11:45", "12:00", "12:15", "12:30", "12:45",
        "13:00", "13:15", "13:30", "13:45", "14:00", "14:15", "14:30",
        "14:45", "15:00", "15:15", "15:30", "15:45"
    ]

    if args.force_initialize is not None:
        if args.force_initialize == -1:
            try:
                plan_count_input = input("Enter the number of plans to initialize: ").strip()
                plan_count = int(plan_count_input)
                if plan_count < 1:
                    raise ValueError("Plan count must be at least 1.")
            except ValueError as ve:
                logging.error(f"Invalid plan count input: {ve}")
                print(f"Error: {ve}")
                conn.close()
                sys.exit(1)
        else:
            plan_count = args.force_initialize

        print("Force initializing the database. Please provide Account IDs.")
        accounts = get_accounts()

        initialize_database(conn, plan_count, force=True, accounts=accounts, times=times)
        conn.close()
        sys.exit(0)

    if args.initialize:
        plan_count = 1
        initialize_database(conn, plan_count, force=False, accounts=[], times=times)
        conn.close()
        sys.exit(0)

    try:
        data = pd.read_csv(csv_path, delimiter=',', quotechar='"')
        logging.info(f"Loaded tradeplan.csv with {len(data)} entries.")
    except FileNotFoundError:
        logging.error("CSV file not found. Please check the file path.")
        print("Error: CSV file not found. Please check the file path.")
        conn.close()
        sys.exit(1)
    except pd.errors.EmptyDataError:
        logging.error("CSV file is empty.")
        print("Error: CSV file is empty.")
        conn.close()
        sys.exit(1)
    except pd.errors.ParserError as e:
        logging.error(f"Error parsing CSV file: {str(e)}")
        print(f"Error parsing CSV file: {str(e)}")
        conn.close()
        sys.exit(1)

    if args.qty is not None:
        data['Qty'] = args.qty
        logging.info(f"Set Qty for all entries to {args.qty}.")
        print(f"Set Qty for all entries to {args.qty}.")
    elif 'Qty' in data.columns and data['Qty'].notna().any():
        logging.info("Using existing 'Qty' column from CSV.")
        print("Using existing 'Qty' column from CSV.")
    else:
        data['Qty'] = 1
        logging.info("Set Qty for all entries to default value 1.")
        print("Set Qty for all entries to default value 1.")

    if args.distribution:
        if 'PnL Rank' not in data.columns:
            logging.error("Missing 'PnL Rank' column in CSV. Cannot distribute contracts.")
            print("Error: 'PnL Rank' column is missing in the CSV file. Cannot distribute contracts.")
            conn.close()
            sys.exit(1)
        data = data.sort_values('PnL Rank')
        data['Qty'] = data['Qty'].fillna(0)
        top_n = 3
        data.loc[data.index[:top_n], 'Qty'] += 1
        logging.info(f"Added 1 to Qty for top {top_n} PnL Rank entries.")
        print(f"Added 1 to Qty for top {top_n} PnL Rank entries.")

        subtract_n = 3
        if len(data) >= 10:
            data.loc[data.index[7:10], 'Qty'] -= 1
            logging.info("Subtracted 1 from Qty for PnL Rank entries 8-10.")
            print("Subtracted 1 from Qty for PnL Rank entries 8-10.")

    if 'Spread' in data.columns:
        data['Spread'] = data['Spread'].astype(str).str.replace('-', ',', regex=False)
        logging.info("Replaced dashes with commas in 'Spread' column.")
        print("Replaced dashes with commas in 'Spread' column.")

    try:
        data.to_csv(csv_path, index=False)
        logging.info("Updated tradeplan.csv saved.")
        print("Updated tradeplan.csv saved.")
    except Exception as e:
        logging.error(f"Error saving updated CSV: {str(e)}")
        print(f"Error saving updated CSV: {str(e)}")
        conn.close()
        sys.exit(1)

    try:
        conn.execute("BEGIN TRANSACTION")
        conn.execute("UPDATE ScheduleMaster SET IsActive = 0")
        conn.commit()
        logging.info("All schedules deactivated.")
        print("All schedules deactivated.")
    except sqlite3.Error as e:
        logging.error(f"Error occurred while deactivating schedules: {str(e)}")
        print(f"Error occurred while deactivating schedules: {str(e)}")
        conn.rollback()
        conn.close()
        sys.exit(1)

    # Create EMA conditions
    # Wrap in transaction at a high level if needed
    conn.execute("BEGIN TRANSACTION")
    try:
        trade_condition_ids = create_trade_conditions(conn)
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        sys.exit(1)

    # Process tradeplan in a transaction
    try:
        process_tradeplan(conn, data, trade_condition_ids)
    except Exception as e:
        print(f"Error processing tradeplan: {e}")
        conn.close()
        sys.exit(1)

    try:
        conn.close()
        logging.info("Database connection closed.")
    except sqlite3.Error as e:
        logging.error(f"Error occurred while closing the database: {str(e)}")

    try:
        os.remove(backup_filepath)
        logging.info(f"Removed unzipped backup file: {backup_filepath}")
        print(f"Removed unzipped backup file: {backup_filepath}")
    except FileNotFoundError:
        logging.warning("Backup file not found. Skipping deletion.")
        print("Warning: Backup file not found. Skipping deletion.")

    logging.info("Script executed successfully.")
    print("Script executed successfully.\n")
    print("Final tradeplan.csv:")
    print(data.to_csv(index=False))


if __name__ == "__main__":
    main()
