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
            formatted_account = f"IB:U{account_input[4:]}"
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
    """
    try:
        for plan in plan_suffixes:
            for time in times:
                put_template = {
                    "Name": f"PUT SPREAD ({time}) {plan}",
                    "TradeType": "PutSpread",
                    "TargetType": "Credit",
                    "TargetMin": 1.0,
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

                call_template = put_template.copy()
                call_template["Name"] = f"CALL SPREAD ({time}) {plan}"
                call_template["TradeType"] = "CallSpread"
                call_template["Strategy"] = f"CALL SPREAD {plan}"
                call_template["TargetMax"] = 0.0  # For CALL irrelevant

                # Insert or update PUT
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
                else:
                    cols = ', '.join(put_template.keys())
                    placeholders = ', '.join(['?'] * len(put_template))
                    vals = tuple(put_template.values())
                    conn.execute(
                        f"INSERT INTO TradeTemplate ({cols}) VALUES ({placeholders})",
                        vals
                    )
                    logging.info(f"Inserted TradeTemplate: {put_template['Name']}")

                # Insert or update CALL
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
                else:
                    cols = ', '.join(call_template.keys())
                    placeholders = ', '.join(['?'] * len(call_template))
                    vals = tuple(call_template.values())
                    conn.execute(
                        f"INSERT INTO TradeTemplate ({cols}) VALUES ({placeholders})",
                        vals
                    )

        logging.info("TradeTemplates created or updated successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error inserting TradeTemplates: {str(e)}")
        raise


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
                    raise ValueError(f"Missing PUT template '{put_name}'")
                put_template_id = row[0]

                # GET CALL template
                call_name = f"CALL SPREAD ({time}) {plan}"
                cursor = conn.execute(
                    "SELECT TradeTemplateID FROM TradeTemplate WHERE Name = ?",
                    (call_name,)
                )
                row = cursor.fetchone()
                if not row:
                    raise ValueError(f"Missing CALL template '{call_name}'")
                call_template_id = row[0]

                # Example conditions
                put_strategy = 'EMA520'
                call_strategy = 'EMA520_INV'
                put_cond = trade_condition_ids.get(put_strategy)
                call_cond = trade_condition_ids.get(call_strategy)
                if not (put_cond and call_cond):
                    raise ValueError("Missing default trade conditions.")

                for account in accounts:
                    # PUT
                    cursor = conn.execute("""
                        SELECT ScheduleMasterID FROM ScheduleMaster
                        WHERE TradeTemplateID=? AND Strategy=? AND Account=?
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
                            )
                            VALUES (
                                ?, ?, 'Trade', 1,
                                ?, ?, 0, 5, ?,
                                0, NULL, ?, ?, ?, ?,
                                1, 1, 1, 1, 1, 0,
                                'FixedQty', 0.0, 0
                            )
                        """, (
                            account, put_template_id,
                            hour, minute, int(active),
                            put_strategy, f"PUT SPREAD {plan}",
                            put_cond['id'], put_cond['description']
                        ))

                    # CALL
                    cursor = conn.execute("""
                        SELECT ScheduleMasterID FROM ScheduleMaster
                        WHERE TradeTemplateID=? AND Strategy=? AND Account=?
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
                            call_strategy, f"CALL SPREAD {plan}",
                            call_cond['id'], call_cond['description']
                        ))

        logging.info("ScheduleMaster entries created or updated successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error inserting ScheduleMaster entries: {str(e)}")
        raise


def verify_put_update(conn, trade_template_id, expected_target_max, expected_long_width, expected_stop_multiple):
    """
    Verify that the PUT Spread Template has correct columns.
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
            if (actual_target_max == expected_target_max
                and actual_long_width == expected_long_width
                and actual_stop_multiple == expected_stop_multiple):
                return True
            else:
                logging.error("Verification FAILED for PUT TradeTemplateID %s", trade_template_id)
                return False
        else:
            return False
    except sqlite3.Error as e:
        logging.error(f"Error verifying PUT: {e}")
        return False


def verify_call_update(conn, trade_template_id, expected_target_max_call, expected_long_width, expected_stop_multiple):
    """
    Verify that the CALL Spread Template has correct columns.
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
            if (actual_tmax_call == expected_target_max_call
                and actual_lwidth == expected_long_width
                and actual_stop_mult == expected_stop_multiple):
                return True
            else:
                logging.error("Verification FAILED for CALL TradeTemplateID %s", trade_template_id)
                return False
        else:
            return False
    except sqlite3.Error as e:
        logging.error(f"Error verifying CALL: {e}")
        return False


def initialize_database(conn, plan_count, force, accounts, times):
    """
    Initialize entire DB: optionally delete old data, create conditions/templates/schedules.
    """
    try:
        conn.execute("BEGIN TRANSACTION")
        if force:
            conn.execute("DELETE FROM TradeTemplate")
            conn.execute("DELETE FROM ScheduleMaster")
            conn.execute("DELETE FROM TradeConditionDetail")
            conn.execute("DELETE FROM TradeCondition")
            logging.info("Cleared existing data from TradeTemplate, ScheduleMaster, TradeConditionDetail, TradeCondition.")
            try:
                conn.execute("DELETE FROM sqlite_sequence WHERE name='TradeCondition'")
            except sqlite3.Error as e:
                logging.warning(f"Could not reset TradeCondition ID sequence: {e}")

        trade_condition_ids = create_trade_conditions(conn)
        plan_suffixes = [f"P{i}" for i in range(1, plan_count + 1)]
        create_trade_templates(conn, plan_suffixes, times)

        if force:
            if not accounts:
                logging.error("No accounts provided for force-initialize.")
                print("Error: No accounts provided for force-initialize.")
                conn.rollback()
                return
            create_schedules(conn, plan_suffixes, trade_condition_ids, accounts, times, active=False)

        conn.commit()
        if force:
            print(f"Initialized DB with TradeTemplates for P1..P{plan_count}, schedules inactive.")
        else:
            print("Inserted missing TradeTemplates and Schedules.")

    except sqlite3.Error as e:
        logging.error(f"Error in initialize_database: {e}")
        conn.rollback()
        raise


def update_put_template(conn, template_name, premium, spread, stop_multiple):
    cursor = conn.execute(
        "SELECT TradeTemplateID FROM TradeTemplate WHERE Name = ?",
        (template_name,)
    )
    row = cursor.fetchone()
    if not row:
        raise ValueError(f"Missing PUT template {template_name}")

    tid = row[0]
    conn.execute("""
        UPDATE TradeTemplate
        SET TargetMax = ?, LongWidth = ?, StopMultiple = ?
        WHERE TradeTemplateID = ?
    """, (premium, spread, stop_multiple, tid))


def update_call_template(conn, template_name, premium, spread, stop_multiple):
    cursor = conn.execute(
        "SELECT TradeTemplateID FROM TradeTemplate WHERE Name = ?",
        (template_name,)
    )
    row = cursor.fetchone()
    if not row:
        raise ValueError(f"Missing CALL template {template_name}")

    tid = row[0]
    conn.execute("""
        UPDATE TradeTemplate
        SET TargetMaxCall = ?, LongWidth = ?, StopMultiple = ?
        WHERE TradeTemplateID = ?
    """, (premium, spread, stop_multiple, tid))


def update_put_schedule_master(conn, template_name, qty_override, ema_strategy,
                               condition_id, condition_desc, plan):
    cursor = conn.execute(
        "SELECT TradeTemplateID FROM TradeTemplate WHERE Name = ?",
        (template_name,)
    )
    row = cursor.fetchone()
    if not row:
        raise ValueError(f"Missing PUT TradeTemplate for schedule {template_name}")
    tid = row[0]

    conn.execute("""
        UPDATE ScheduleMaster
        SET IsActive = 1, QtyOverride = ?, Strategy = ?,
            TradeConditionID = ?, DisplayStrategy = ?,
            DisplayCondition = ?
        WHERE TradeTemplateID = ?
    """, (
        qty_override, ema_strategy, condition_id,
        f"PUT SPREAD {plan}", condition_desc, tid
    ))


def update_call_schedule_master(conn, template_name, qty_override, ema_strategy,
                                condition_id, condition_desc, plan):
    cursor = conn.execute(
        "SELECT TradeTemplateID FROM TradeTemplate WHERE Name = ?",
        (template_name,)
    )
    row = cursor.fetchone()
    if not row:
        raise ValueError(f"Missing CALL TradeTemplate for schedule {template_name}")
    tid = row[0]

    conn.execute("""
        UPDATE ScheduleMaster
        SET IsActive = 1, QtyOverride = ?, Strategy = ?,
            TradeConditionID = ?, DisplayStrategy = ?,
            DisplayCondition = ?
        WHERE TradeTemplateID = ?
    """, (
        qty_override, ema_strategy, condition_id,
        f"CALL SPREAD {plan}", condition_desc, tid
    ))


def process_tradeplan(conn, data, trade_condition_ids):
    """
    Reads the DataFrame row by row, updates the relevant templates & schedules.
    """

    # 1) Normalize Plan
    if "Plan" in data.columns:
        # Falls es leere Einträge gibt, erst auffüllen
        data['Plan'] = data['Plan'].fillna('').astype(str).str.upper()
    else:
        data['Plan'] = 'P1'

    # 2) OptionType vorhanden?
    has_option_type = ("OptionType" in data.columns)

    # 3) Strategie-Check
    required = set()
    for strategy in data['Strategy'].unique():
        s_up = str(strategy).upper()  # Falls NaN
        if s_up in ["EMA520", "EMA540", "EMA2040"]:
            required.add(s_up)
            required.add(f"{s_up}_INV")
        else:
            raise ValueError(f"Unsupported Strategy '{strategy}'")

    missing = required - set(trade_condition_ids.keys())
    if missing:
        raise ValueError(f"Missing TradeConditions for {', '.join(missing)}")

    # 4) Durch alle Zeilen
    for idx, row in data.iterrows():
        plan = str(row['Plan'])
        hour_minute = row['Hour:Minute']

        # premium (float)
        premium = float(row['Premium'])
        # Spread => schon string, dashes wurden ersetzt. Aber sicherheitshalber:
        spread = str(row['Spread'])
        # Stop
        stop_str = str(row['Stop'])
        if stop_str.lower().endswith('x'):
            stop_multiple = float(stop_str[:-1])
        else:
            stop_multiple = float(stop_str)

        qty_override = int(row['Qty'])

        # Condition ID
        ema_strat = str(row['Strategy']).upper()
        if ema_strat == "EMA520":
            cond_id_put  = trade_condition_ids["EMA520"]['id']
            cond_desc_put  = trade_condition_ids["EMA520"]['description']
            cond_id_call = trade_condition_ids["EMA520_INV"]['id']
            cond_desc_call = trade_condition_ids["EMA520_INV"]['description']
        elif ema_strat == "EMA540":
            cond_id_put  = trade_condition_ids["EMA540"]['id']
            cond_desc_put  = trade_condition_ids["EMA540"]['description']
            cond_id_call = trade_condition_ids["EMA540_INV"]['id']
            cond_desc_call = trade_condition_ids["EMA540_INV"]['description']
        else:  # "EMA2040"
            cond_id_put  = trade_condition_ids["EMA2040"]['id']
            cond_desc_put  = trade_condition_ids["EMA2040"]['description']
            cond_id_call = trade_condition_ids["EMA2040_INV"]['id']
            cond_desc_call = trade_condition_ids["EMA2040_INV"]['description']

        if has_option_type:
            opt_type = str(row['OptionType']).upper().strip()
            if opt_type == 'P':
                put_tmpl_name = f"PUT SPREAD ({hour_minute}) {plan}"
                update_put_template(conn, put_tmpl_name, premium, spread, stop_multiple)
                update_put_schedule_master(
                    conn, put_tmpl_name, qty_override,
                    ema_strat, cond_id_put, cond_desc_put, plan
                )
            elif opt_type == 'C':
                call_tmpl_name = f"CALL SPREAD ({hour_minute}) {plan}"
                update_call_template(conn, call_tmpl_name, premium, spread, stop_multiple)
                update_call_schedule_master(
                    conn, call_tmpl_name, qty_override,
                    ema_strat + "_INV", cond_id_call, cond_desc_call, plan
                )
            else:
                raise ValueError(f"Invalid OptionType '{opt_type}' in row {idx+1}")
        else:
            # Legacy -> beides updaten
            put_tmpl_name = f"PUT SPREAD ({hour_minute}) {plan}"
            update_put_template(conn, put_tmpl_name, premium, spread, stop_multiple)
            update_put_schedule_master(
                conn, put_tmpl_name, qty_override,
                ema_strat, cond_id_put, cond_desc_put, plan
            )

            call_tmpl_name = f"CALL SPREAD ({hour_minute}) {plan}"
            update_call_template(conn, call_tmpl_name, premium, spread, stop_multiple)
            update_call_schedule_master(
                conn, call_tmpl_name, qty_override,
                ema_strat + "_INV", cond_id_call, cond_desc_call, plan
            )


def main():
    args = parse_arguments()
    setup_logging()

    db_path = os.path.abspath('data.db3')
    csv_path = os.path.abspath('tradeplan.csv')
    backup_dir = os.path.abspath('tradeplan-backup')

    if not os.path.exists(backup_dir):
        try:
            os.makedirs(backup_dir)
            logging.info(f"Created backup directory at: {backup_dir}")
            print(f"Created backup directory at: {backup_dir}")
        except OSError as e:
            logging.error(f"Unable to create backup directory. {str(e)}")
            print(f"Error: Unable to create backup directory. {str(e)}")
            sys.exit(1)

    # Backup
    backup_filepath = create_backup(db_path, backup_dir)

    # DB connect
    conn = connect_database(db_path)

    # Default times
    times = [
        "09:33", "09:45", "10:00", "10:15", "10:30", "10:45", "11:00",
        "11:15", "11:30", "11:45", "12:00", "12:15", "12:30", "12:45",
        "13:00", "13:15", "13:30", "13:45", "14:00", "14:15", "14:30",
        "14:45", "15:00", "15:15", "15:30", "15:45"
    ]

    # Force init?
    if args.force_initialize is not None:
        if args.force_initialize == -1:
            try:
                plan_count = int(input("Enter the number of plans to initialize: ").strip())
                if plan_count < 1:
                    raise ValueError("Plan count must be >= 1")
            except ValueError as ve:
                print(f"Error: {ve}")
                conn.close()
                sys.exit(1)
        else:
            plan_count = args.force_initialize

        print("Force-initializing the database. Please provide Account IDs.")
        accounts = get_accounts()
        try:
            initialize_database(conn, plan_count, force=True, accounts=accounts, times=times)
        except Exception as ex:
            print(f"Error during force-initialize: {ex}")
        finally:
            conn.close()
        sys.exit(0)

    # Normal init?
    if args.initialize:
        try:
            initialize_database(conn, plan_count=1, force=False, accounts=[], times=times)
        except Exception as ex:
            print(f"Error during initialize: {ex}")
        finally:
            conn.close()
        sys.exit(0)

    # --- READ CSV ---
    try:
        data = pd.read_csv(csv_path, delimiter=',', quotechar='"')
        logging.info(f"Loaded tradeplan.csv with {len(data)} entries.")
        print(f"Loaded tradeplan.csv with {len(data)} entries.")
    except FileNotFoundError:
        logging.error("CSV file not found.")
        print("Error: CSV file not found.")
        conn.close()
        sys.exit(1)
    except pd.errors.EmptyDataError:
        logging.error("CSV file is empty.")
        print("Error: CSV file is empty.")
        conn.close()
        sys.exit(1)
    except pd.errors.ParserError as e:
        logging.error(f"Error parsing CSV: {e}")
        print(f"Error parsing CSV: {e}")
        conn.close()
        sys.exit(1)

    # Qty
    if args.qty is not None:
        data['Qty'] = args.qty
        logging.info(f"Set Qty for all entries to {args.qty}.")
        print(f"Set Qty for all entries to {args.qty}.")
    elif 'Qty' not in data.columns or data['Qty'].isna().all():
        data['Qty'] = 1
        logging.info("Set Qty to default 1.")
        print("Set Qty to default 1.")
    else:
        print("Using existing 'Qty' column from CSV.")
        logging.info("Using existing 'Qty' from CSV.")

    # Distribution?
    if args.distribution:
        if 'PnL Rank' not in data.columns:
            logging.error("Missing 'PnL Rank' column for distribution.")
            print("Error: Missing 'PnL Rank' column. Cannot distribute.")
            conn.close()
            sys.exit(1)
        data = data.sort_values('PnL Rank')
        data['Qty'] = data['Qty'].fillna(0)
        top_n = 3
        data.loc[data.index[:top_n], 'Qty'] += 1
        logging.info(f"Added 1 to Qty for top {top_n} PnL.")
        print(f"Added 1 to Qty for top {top_n} PnL.")
        if len(data) >= 10:
            data.loc[data.index[7:10], 'Qty'] -= 1
            logging.info("Subtracted 1 from Qty for ranks 8-10.")
            print("Subtracted 1 from Qty for ranks 8-10.")

    # FIX HERE: Ensure `Spread` is always string before .str.replace
    if 'Spread' in data.columns:
        data['Spread'] = data['Spread'].fillna('').astype(str)
        data['Spread'] = data['Spread'].str.replace('-', ',', regex=False)
        logging.info("Replaced dashes in 'Spread' column.")
        print("Replaced dashes in 'Spread' column.")

    # Save CSV
    try:
        data.to_csv(csv_path, index=False)
        logging.info("Updated tradeplan.csv saved.")
        print("Updated tradeplan.csv saved.")
    except Exception as e:
        logging.error(f"Error saving updated CSV: {str(e)}")
        print(f"Error saving updated CSV: {str(e)}")
        conn.close()
        sys.exit(1)

    # Deactivate schedules
    try:
        conn.execute("BEGIN TRANSACTION")
        conn.execute("UPDATE ScheduleMaster SET IsActive=0")
        conn.commit()
        logging.info("All schedules deactivated.")
        print("All schedules deactivated.")
    except sqlite3.Error as e:
        print(f"Error deactivating schedules: {e}")
        logging.error(f"Error deactivating schedules: {e}")
        conn.rollback()
        conn.close()
        sys.exit(1)

    # Create trade conditions
    try:
        conn.execute("BEGIN TRANSACTION")
        trade_condition_ids = create_trade_conditions(conn)
        conn.commit()
    except Exception as e:
        print(f"Error creating trade conditions: {e}")
        logging.error(f"Error creating trade conditions: {e}")
        conn.rollback()
        conn.close()
        sys.exit(1)

    # Process the data
    try:
        conn.execute("BEGIN TRANSACTION")
        process_tradeplan(conn, data, trade_condition_ids)
        conn.commit()
        logging.info("TradeTemplates and Schedules updated successfully.")
        print("\nTradeTemplates and Schedules updated successfully.")
    except Exception as e:
        print(f"Error occurred while processing tradeplan: {e}")
        logging.error(f"Error processing tradeplan: {e}")
        conn.rollback()
        conn.close()
        sys.exit(1)

    # Close DB
    try:
        conn.close()
        logging.info("DB connection closed.")
    except sqlite3.Error as e:
        logging.error(f"Error closing DB: {e}")

    # Remove unzipped backup
    try:
        os.remove(backup_filepath)
        logging.info(f"Removed unzipped backup: {backup_filepath}")
        print(f"Removed unzipped backup file: {backup_filepath}")
    except FileNotFoundError:
        print("Warning: Backup file not found. Skipping deletion.")
        logging.warning("No backup file to remove.")

    logging.info("Script executed successfully.")
    print("Script executed successfully.\n")
    print("Final tradeplan.csv:")
    print(data.to_csv(index=False))


if __name__ == "__main__":
    main()
