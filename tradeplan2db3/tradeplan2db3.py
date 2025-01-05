#!/usr/bin/env python3
"""
tradeplan2db3.py

Fix for "cannot start a transaction within a transaction".
Removes nested transactions in child functions, so that
only the parent functions handle BEGIN/COMMIT/ROLLBACK.
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
    parser = argparse.ArgumentParser(description='Process tradeplan.')
    parser.add_argument('--qty', type=int, help='Set quantity for all entry times')
    parser.add_argument('--distribution', action='store_true', help='Distribute contracts based on PnL Rank')
    parser.add_argument(
        '--force-initialize',
        type=int,
        nargs='?',
        const=-1,  # Sentinel value if user does not provide a plan count
        help=(
            'Force initialize DB by deleting existing TradeTemplates and Schedules. '
            'If no number is provided, the script will prompt for one.'
        )
    )
    parser.add_argument(
        '--initialize',
        action='store_true',
        help=(
            'Initialize DB by inserting missing TradeConditions, TradeTemplates, and ScheduleMaster '
            'entries without deleting existing data.'
        )
    )
    return parser.parse_args()


def setup_logging():
    logging.basicConfig(
        filename='tradeplan_updates.log',
        filemode='a',
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
    Prompt the user to input up to 3 IB account IDs (e.g. IB:U1234567).
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
                f"Invalid format. Please enter 'IB:U########' (e.g., {example_account})."
            )
            continue

        # Normalize the account to IB:U########
        if account_input.startswith("IB:U") and len(account_input) in [11, 12]:
            formatted_account = account_input
        elif account_input.startswith("IB:") and len(account_input) in [10, 11]:
            # Missing 'U'
            formatted_account = "IB:U" + account_input[4:]
        elif account_input.startswith("U") and len(account_input) in [8, 9]:
            # Missing 'IB:'
            formatted_account = "IB:" + account_input
        elif account_input.isdigit() and len(account_input) in [7, 8]:
            # Missing 'IB:U'
            formatted_account = "IB:U" + account_input
        else:
            print(
                f"Invalid format for Account ID. Please enter 'IB:U########'."
            )
            continue

        accounts.append(formatted_account)
        print(f"Added Account: {formatted_account}")
        logging.info(f"Added Account: {formatted_account}")

        if len(accounts) < max_accounts:
            more = input("Do you want to add another account? (y/n): ").strip().lower()
            if more != 'y':
                break

    if not accounts:
        print("No valid accounts provided. Exiting.")
        logging.error("No valid accounts provided.")
        sys.exit(1)

    return accounts


def create_trade_conditions(conn):
    """
    Insert or confirm the default EMA conditions, setting RetryUntilExpiration=0.
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
        # No BEGIN TRANSACTION here
        for name, (desc, operator, input_val, comparison) in conditions.items():
            cursor = conn.execute(
                "SELECT TradeConditionID FROM TradeCondition WHERE Name = ?",
                (desc,)
            )
            found = cursor.fetchone()
            if found:
                condition_id = found[0]
                logging.info(f"TradeCondition already exists: {desc} with ID {condition_id}")
            else:
                # Insert
                cursor = conn.execute(
                    "INSERT INTO TradeCondition (Name, RetryUntilExpiration) VALUES (?, ?)",
                    (desc, 0)
                )
                condition_id = cursor.lastrowid
                logging.info(f"Inserted TradeCondition: {desc} with ID {condition_id}")

                # Insert detail
                conn.execute("""
                    INSERT INTO TradeConditionDetail (
                        TradeConditionID, [Group], Input, Operator, Comparison, ComparisonType
                    ) VALUES (?, 1, ?, ?, ?, 'Input')
                """, (condition_id, input_val, operator, comparison))
                logging.info(f"Inserted TradeConditionDetail for ID {condition_id}")

            trade_condition_ids[name] = {
                "id": condition_id,
                "description": desc
            }

        # Update existing conditions
        conn.execute("UPDATE TradeCondition SET RetryUntilExpiration = 0")
        logging.info("Set 'RetryUntilExpiration' to 0 for all TradeConditions.")

        conn.execute("UPDATE TradeConditionDetail SET ComparisonType = 'Input'")
        logging.info("Set 'ComparisonType' to 'Input' for all TradeConditionDetail entries.")

        logging.info("All default EMA conditions processed successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error inserting/updating TradeConditions: {str(e)}")
        raise

    return trade_condition_ids


def create_trade_templates(conn, plan_suffixes, times):
    """
    Create or update PUT and CALL templates for each plan and time.
    """
    try:
        for plan in plan_suffixes:
            for t in times:
                put_name = f"PUT SPREAD ({t}) {plan}"
                call_name = f"CALL SPREAD ({t}) {plan}"

                # PUT template dict
                put_template = {
                    "Name": put_name,
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
                call_template["Name"] = call_name
                call_template["TradeType"] = "CallSpread"
                call_template["Strategy"] = f"CALL SPREAD {plan}"
                # For example, set target max to 0 for calls
                call_template["TargetMax"] = 0.0

                # Check/insert PUT
                cursor = conn.execute(
                    "SELECT TradeTemplateID, IsDeleted FROM TradeTemplate WHERE Name = ?",
                    (put_name,)
                )
                existing = cursor.fetchone()
                if existing:
                    existing_id, is_del = existing
                    logging.info(f"PUT template already exists: {put_name} (ID: {existing_id})")
                    if is_del != 0:
                        conn.execute("UPDATE TradeTemplate SET IsDeleted=0 WHERE TradeTemplateID=?", (existing_id,))
                else:
                    cols = ', '.join(put_template.keys())
                    placeholders = ', '.join(['?'] * len(put_template))
                    vals = tuple(put_template.values())
                    conn.execute(f"INSERT INTO TradeTemplate ({cols}) VALUES ({placeholders})", vals)
                    logging.info(f"Inserted PUT template: {put_name}")

                # Check/insert CALL
                cursor = conn.execute(
                    "SELECT TradeTemplateID, IsDeleted FROM TradeTemplate WHERE Name = ?",
                    (call_name,)
                )
                existing = cursor.fetchone()
                if existing:
                    existing_id, is_del = existing
                    logging.info(f"CALL template already exists: {call_name} (ID: {existing_id})")
                    if is_del != 0:
                        conn.execute("UPDATE TradeTemplate SET IsDeleted=0 WHERE TradeTemplateID=?", (existing_id,))
                else:
                    cols = ', '.join(call_template.keys())
                    placeholders = ', '.join(['?'] * len(call_template))
                    vals = tuple(call_template.values())
                    conn.execute(f"INSERT INTO TradeTemplate ({cols}) VALUES ({placeholders})", vals)
                    logging.info(f"Inserted CALL template: {call_name}")

        logging.info("TradeTemplates created or updated successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error inserting/updating TradeTemplates: {str(e)}")
        raise


def create_schedules(conn, plan_suffixes, trade_condition_ids, accounts, times, active=True):
    """
    Create or update ScheduleMaster entries for each plan/time/account.
    """
    try:
        for plan in plan_suffixes:
            for t in times:
                hour, minute = map(int, t.split(':'))
                put_name = f"PUT SPREAD ({t}) {plan}"
                call_name = f"CALL SPREAD ({t}) {plan}"

                # Lookup the template IDs
                cursor = conn.execute(
                    "SELECT TradeTemplateID FROM TradeTemplate WHERE Name = ?", (put_name,)
                )
                put_result = cursor.fetchone()
                if not put_result:
                    raise ValueError(f"Missing PUT template: {put_name}")
                put_template_id = put_result[0]

                cursor = conn.execute(
                    "SELECT TradeTemplateID FROM TradeTemplate WHERE Name = ?", (call_name,)
                )
                call_result = cursor.fetchone()
                if not call_result:
                    raise ValueError(f"Missing CALL template: {call_name}")
                call_template_id = call_result[0]

                # Example default strategies
                put_strategy_key = "EMA520"
                call_strategy_key = "EMA520_INV"
                put_trade_condition = trade_condition_ids.get(put_strategy_key)
                call_trade_condition = trade_condition_ids.get(call_strategy_key)
                if not put_trade_condition or not call_trade_condition:
                    raise ValueError("Missing trade conditions for default scheduling.")

                display_strategy_put = f"PUT SPREAD {plan}"
                display_condition_put = put_trade_condition["description"]
                display_strategy_call = f"CALL SPREAD {plan}"
                display_condition_call = call_trade_condition["description"]

                for acct in accounts:
                    # Insert or confirm schedule for PUT
                    cursor = conn.execute("""
                        SELECT ScheduleMasterID FROM ScheduleMaster
                        WHERE TradeTemplateID=? AND Strategy=? AND Account=?
                    """, (put_template_id, put_strategy_key, acct))
                    if cursor.fetchone():
                        logging.info(f"ScheduleMaster already exists (PUT: {put_name}, {acct})")
                    else:
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
                                ?, ?, 0, 5, ?,
                                0, NULL, ?, ?,
                                ?, ?,
                                1,1,1,1,1,0,
                                'FixedQty', 0.0, 0
                            )
                        """, (
                            acct, put_template_id, "Trade", 1,
                            hour, minute, int(active),
                            put_strategy_key, display_strategy_put,
                            put_trade_condition["id"], display_condition_put
                        ))
                        logging.info(f"Inserted schedule for PUT: {put_name}, {acct}")

                    # Insert or confirm schedule for CALL
                    cursor = conn.execute("""
                        SELECT ScheduleMasterID FROM ScheduleMaster
                        WHERE TradeTemplateID=? AND Strategy=? AND Account=?
                    """, (call_template_id, call_strategy_key, acct))
                    if cursor.fetchone():
                        logging.info(f"ScheduleMaster already exists (CALL: {call_name}, {acct})")
                    else:
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
                                ?, ?, 0, 5, ?,
                                0, NULL, ?, ?,
                                ?, ?,
                                1,1,1,1,1,0,
                                'FixedQty', 0.0, 0
                            )
                        """, (
                            acct, call_template_id, "Trade", 1,
                            hour, minute, int(active),
                            call_strategy_key, display_strategy_call,
                            call_trade_condition["id"], display_condition_call
                        ))
                        logging.info(f"Inserted schedule for CALL: {call_name}, {acct}")

        logging.info("ScheduleMaster entries created or updated successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error inserting/updating Schedules: {str(e)}")
        raise


def verify_put_update(conn, template_id, expected_target_max, expected_long_width, expected_stop_multiple):
    """
    Verify that a PUT spread template was updated as expected.
    """
    try:
        c = conn.execute("""
            SELECT TargetMax, LongWidth, StopMultiple FROM TradeTemplate
            WHERE TradeTemplateID = ?
        """, (template_id,))
        row = c.fetchone()
        if not row:
            logging.error(f"PUT template {template_id} not found for verification.")
            return False
        actual_max, actual_width, actual_stop = row
        if (actual_max == expected_target_max and
            actual_width == expected_long_width and
            actual_stop == expected_stop_multiple):
            logging.info(f"PUT verify OK (ID {template_id})")
            return True
        else:
            logging.error(f"PUT verify FAILED (ID {template_id}) - got {row}")
            return False
    except sqlite3.Error as e:
        logging.error(f"PUT verification error: {str(e)}")
        return False


def verify_call_update(conn, template_id, expected_target_max_call, expected_long_width, expected_stop_multiple):
    """
    Verify that a CALL spread template was updated as expected.
    """
    try:
        c = conn.execute("""
            SELECT TargetMaxCall, LongWidth, StopMultiple FROM TradeTemplate
            WHERE TradeTemplateID = ?
        """, (template_id,))
        row = c.fetchone()
        if not row:
            logging.error(f"CALL template {template_id} not found for verification.")
            return False
        actual_call_max, actual_width, actual_stop = row
        if (actual_call_max == expected_target_max_call and
            actual_width == expected_long_width and
            actual_stop == expected_stop_multiple):
            logging.info(f"CALL verify OK (ID {template_id})")
            return True
        else:
            logging.error(f"CALL verify FAILED (ID {template_id}) - got {row}")
            return False
    except sqlite3.Error as e:
        logging.error(f"CALL verification error: {str(e)}")
        return False


def initialize_database(conn, plan_count, force, accounts, times):
    """
    (Optional) Wipe out existing data or just fill missing. Then create conditions, templates, schedules.
    """
    try:
        # Start transaction
        conn.execute("BEGIN TRANSACTION")
        if force:
            conn.execute("DELETE FROM TradeTemplate")
            conn.execute("DELETE FROM ScheduleMaster")
            conn.execute("DELETE FROM TradeConditionDetail")
            conn.execute("DELETE FROM TradeCondition")
            logging.info("Cleared existing TradeTemplates, Schedules, and TradeConditions.")

            try:
                # If using AUTOINCREMENT
                conn.execute("DELETE FROM sqlite_sequence WHERE name='TradeCondition'")
                logging.info("Reset TradeCondition ID sequence.")
            except sqlite3.Error as e:
                logging.warning(f"Could not reset TradeCondition ID sequence. {str(e)}")
        else:
            logging.info("Inserting missing data (TradeConditions, TradeTemplates, Schedules).")

        # Create default trade conditions
        trade_condition_ids = create_trade_conditions(conn)

        # Create templates for P1..P(plan_count)
        plan_suffixes = [f"P{i}" for i in range(1, plan_count + 1)]
        create_trade_templates(conn, plan_suffixes, times)

        # Create schedules (inactive if force is True)
        if force:
            if not accounts:
                logging.error("No accounts provided for force-initialize.")
                print("Error: No accounts provided for force-initialize.")
                conn.rollback()
                conn.close()
                sys.exit(1)
            create_schedules(conn, plan_suffixes, trade_condition_ids, accounts, times, active=False)

        # Commit
        conn.commit()

        if force:
            logging.info(f"Initialized DB with P1..P{plan_count} (Schedules inactive).")
            print(f"Initialized DB with P1..P{plan_count}, schedules inactive.")
        else:
            logging.info(f"Inserted missing templates and schedules for P1..P{plan_count}.")
            print(f"Inserted missing templates and schedules for P1..P{plan_count}.")

    except sqlite3.Error as e:
        logging.error(f"Error in initialize_database: {str(e)}")
        conn.rollback()
        conn.close()
        sys.exit(1)


def update_put_template(conn, template_name, premium, spread, stop_multiple):
    """
    Update PUT template, then verify.
    """
    cur = conn.execute("SELECT TradeTemplateID FROM TradeTemplate WHERE Name=?", (template_name,))
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Missing PUT template {template_name}")
    template_id = row[0]

    conn.execute("""
        UPDATE TradeTemplate
        SET TargetMax=?, LongWidth=?, StopMultiple=?
        WHERE TradeTemplateID=?
    """, (premium, spread, stop_multiple, template_id))

    # Verify
    if verify_put_update(conn, template_id, premium, spread, stop_multiple):
        logging.info(f"Updated and verified PUT template '{template_name}'.")
    else:
        raise ValueError(f"Verification failed for PUT '{template_name}'.")


def update_call_template(conn, template_name, premium, spread, stop_multiple):
    """
    Update CALL template, then verify.
    """
    cur = conn.execute("SELECT TradeTemplateID FROM TradeTemplate WHERE Name=?", (template_name,))
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Missing CALL template {template_name}")
    template_id = row[0]

    conn.execute("""
        UPDATE TradeTemplate
        SET TargetMaxCall=?, LongWidth=?, StopMultiple=?
        WHERE TradeTemplateID=?
    """, (premium, spread, stop_multiple, template_id))

    # Verify
    if verify_call_update(conn, template_id, premium, spread, stop_multiple):
        logging.info(f"Updated and verified CALL template '{template_name}'.")
    else:
        raise ValueError(f"Verification failed for CALL '{template_name}'.")


def update_put_schedule_master(conn, template_name, qty_override, ema_strategy,
                               condition_id, condition_desc, plan):
    """
    Activate and set up the PUT schedule.
    """
    cur = conn.execute("SELECT TradeTemplateID FROM TradeTemplate WHERE Name=?", (template_name,))
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Missing PUT template for schedule: {template_name}")
    template_id = row[0]

    conn.execute("""
        UPDATE ScheduleMaster
        SET IsActive=1, QtyOverride=?, Strategy=?,
            TradeConditionID=?, DisplayStrategy=?,
            DisplayCondition=?
        WHERE TradeTemplateID=?
    """, (
        qty_override, ema_strategy,
        condition_id, f"PUT SPREAD {plan}",
        condition_desc, template_id
    ))
    logging.info(f"Updated ScheduleMaster for PUT '{template_name}'.")


def update_call_schedule_master(conn, template_name, qty_override, ema_strategy,
                                condition_id, condition_desc, plan):
    """
    Activate and set up the CALL schedule.
    """
    cur = conn.execute("SELECT TradeTemplateID FROM TradeTemplate WHERE Name=?", (template_name,))
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Missing CALL template for schedule: {template_name}")
    template_id = row[0]

    conn.execute("""
        UPDATE ScheduleMaster
        SET IsActive=1, QtyOverride=?, Strategy=?,
            TradeConditionID=?, DisplayStrategy=?,
            DisplayCondition=?
        WHERE TradeTemplateID=?
    """, (
        qty_override, ema_strategy,
        condition_id, f"CALL SPREAD {plan}",
        condition_desc, template_id
    ))
    logging.info(f"Updated ScheduleMaster for CALL '{template_name}'.")


def process_tradeplan(conn, data, trade_condition_ids):
    """
    Reads the tradeplan.csv content (in 'data'), then updates templates & schedules.
    This function does a single BEGIN TRANSACTION for all rows, committing at the end.
    """
    # Check if there's an OptionType column
    has_option_type = ("OptionType" in data.columns)

    # Ensure strategies exist in trade_condition_ids
    required_conditions = set()
    for strategy in data['Strategy'].unique():
        s_up = strategy.upper()
        if s_up in ["EMA520", "EMA540", "EMA2040"]:
            required_conditions.add(s_up)
            required_conditions.add(s_up + "_INV")
        else:
            logging.error(f"Unsupported Strategy '{strategy}'.")
            print(f"Error: Unsupported Strategy '{strategy}'.")
            sys.exit(1)

    missing = required_conditions - set(trade_condition_ids.keys())
    if missing:
        print(f"Error: Missing conditions {missing}.")
        sys.exit(1)

    try:
        # Start transaction
        conn.execute("BEGIN TRANSACTION")

        for idx, row in data.iterrows():
            plan = str(row.get('Plan', 'P1')).upper()  # default P1 if missing
            hour_minute = str(row['Hour:Minute']).strip()
            ema_strat = str(row['Strategy']).upper()

            premium = float(row['Premium'])
            spread = str(row['Spread']).replace('-', ',')
            stop_s = str(row['Stop'])
            if stop_s.lower().endswith('x'):
                stop_multiple = float(stop_s[:-1])
            else:
                stop_multiple = float(stop_s)

            qty = int(row['Qty'])

            # Condition IDs
            if ema_strat == "EMA520":
                cond_id_put = trade_condition_ids["EMA520"]["id"]
                cond_desc_put = trade_condition_ids["EMA520"]["description"]
                cond_id_call = trade_condition_ids["EMA520_INV"]["id"]
                cond_desc_call = trade_condition_ids["EMA520_INV"]["description"]
            elif ema_strat == "EMA540":
                cond_id_put = trade_condition_ids["EMA540"]["id"]
                cond_desc_put = trade_condition_ids["EMA540"]["description"]
                cond_id_call = trade_condition_ids["EMA540_INV"]["id"]
                cond_desc_call = trade_condition_ids["EMA540_INV"]["description"]
            elif ema_strat == "EMA2040":
                cond_id_put = trade_condition_ids["EMA2040"]["id"]
                cond_desc_put = trade_condition_ids["EMA2040"]["description"]
                cond_id_call = trade_condition_ids["EMA2040_INV"]["id"]
                cond_desc_call = trade_condition_ids["EMA2040_INV"]["description"]
            else:
                # Should never happen if we validated earlier
                raise ValueError(f"Unknown strategy {ema_strat}")

            put_template_name = f"PUT SPREAD ({hour_minute}) {plan}"
            call_template_name = f"CALL SPREAD ({hour_minute}) {plan}"

            if has_option_type:
                opt_type = str(row['OptionType']).upper()
                if opt_type == 'P':
                    # Update PUT only
                    update_put_template(conn, put_template_name, premium, spread, stop_multiple)
                    update_put_schedule_master(conn, put_template_name, qty,
                                               ema_strat, cond_id_put, cond_desc_put, plan)
                elif opt_type == 'C':
                    # Update CALL only
                    update_call_template(conn, call_template_name, premium, spread, stop_multiple)
                    update_call_schedule_master(conn, call_template_name, qty,
                                                ema_strat + "_INV", cond_id_call, cond_desc_call, plan)
                else:
                    logging.error(f"Invalid OptionType '{opt_type}' at row {idx+1}")
                    print(f"Error: Invalid OptionType '{opt_type}' at row {idx+1}.")
                    conn.rollback()
                    sys.exit(1)
            else:
                # Legacy path: update both
                update_put_template(conn, put_template_name, premium, spread, stop_multiple)
                update_put_schedule_master(conn, put_template_name, qty,
                                           ema_strat, cond_id_put, cond_desc_put, plan)
                update_call_template(conn, call_template_name, premium, spread, stop_multiple)
                update_call_schedule_master(conn, call_template_name, qty,
                                            ema_strat + "_INV", cond_id_call, cond_desc_call, plan)

        # Commit
        conn.commit()
        logging.info("TradeTemplates and ScheduleMaster updated successfully.")
        print("TradeTemplates and ScheduleMaster updated successfully.\n")

    except (sqlite3.Error, ValueError) as e:
        logging.error(f"Error occurred while processing tradeplan: {str(e)}")
        conn.rollback()
        print(f"Error: {str(e)}")
        sys.exit(1)


def main():
    args = parse_arguments()
    setup_logging()

    # Paths
    db_path = os.path.abspath("data.db3")
    csv_path = "tradeplan.csv"

    # Prepare backup directory
    backup_dir_path = os.path.abspath("tradeplan-backup")
    if not os.path.exists(backup_dir_path):
        try:
            os.makedirs(backup_dir_path)
            logging.info(f"Created backup directory at: {backup_dir_path}")
            print(f"Created backup directory at: {backup_dir_path}")
        except OSError as e:
            logging.error(f"Unable to create backup directory. {str(e)}")
            print(f"Error: Unable to create backup directory. {str(e)}")
            sys.exit(1)

    # Create backup
    backup_filepath = create_backup(db_path, backup_dir_path)

    # Connect DB
    conn = connect_database(db_path)

    # Times
    times = [
        "09:33", "09:45", "10:00", "10:15", "10:30", "10:45", "11:00",
        "11:15", "11:30", "11:45", "12:00", "12:15", "12:30", "12:45",
        "13:00", "13:15", "13:30", "13:45", "14:00", "14:15", "14:30",
        "14:45", "15:00", "15:15", "15:30", "15:45"
    ]

    # Handle --force-initialize
    if args.force_initialize is not None:
        if args.force_initialize == -1:
            try:
                user_input = input("Enter the number of plans to initialize: ").strip()
                plan_count = int(user_input)
                if plan_count < 1:
                    raise ValueError("Plan count must be at least 1.")
            except ValueError as e:
                logging.error(f"Invalid plan count: {e}")
                print(f"Error: {e}")
                conn.close()
                sys.exit(1)
        else:
            plan_count = args.force_initialize

        print("Force initializing the database. Provide up to 3 IB account IDs.")
        accounts = get_accounts()

        initialize_database(conn, plan_count, force=True, accounts=accounts, times=times)
        conn.close()
        sys.exit(0)

    # Handle --initialize
    if args.initialize:
        plan_count = 1  # or any default you like
        initialize_database(conn, plan_count, force=False, accounts=[], times=times)
        conn.close()
        sys.exit(0)

    # Read CSV
    try:
        data = pd.read_csv(csv_path, delimiter=',', quotechar='"')
        logging.info(f"Loaded {csv_path} with {len(data)} entries.")
        print(f"Loaded {csv_path} with {len(data)} entries.")
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
        logging.error(f"Error parsing CSV: {str(e)}")
        print(f"Error parsing CSV: {str(e)}")
        conn.close()
        sys.exit(1)

    # Possibly override or ensure 'Qty' column
    if args.qty is not None:
        data['Qty'] = args.qty
        logging.info(f"Set Qty for all entries to {args.qty}.")
        print(f"Set Qty for all entries to {args.qty}.")
    elif 'Qty' not in data.columns or data['Qty'].isna().all():
        data['Qty'] = 1
        logging.info("Set Qty to 1 by default.")
        print("Set Qty to 1 by default.")
    else:
        logging.info("Using existing 'Qty' column from CSV.")
        print("Using existing 'Qty' column from CSV.")

    # --distribution => adjust Qty for top/bottom ranks
    if args.distribution:
        if 'PnL Rank' not in data.columns:
            logging.error("Missing 'PnL Rank' column. Cannot distribute.")
            print("Error: Missing 'PnL Rank' column. Cannot distribute.")
            conn.close()
            sys.exit(1)
        data = data.sort_values('PnL Rank')
        data['Qty'] = data['Qty'].fillna(0)
        top_n = 3
        data.loc[data.index[:top_n], 'Qty'] += 1
        logging.info(f"Distributed +1 Qty to top {top_n} rows by PnL Rank.")
        print(f"Distributed +1 Qty to top {top_n} rows.")
        if len(data) >= 10:
            data.loc[data.index[7:10], 'Qty'] -= 1
            logging.info("Subtracted 1 from Qty for rank entries 8-10.")
            print("Subtracted 1 from Qty for rank entries 8-10.")

    # If there's a Spread column, replace dashes
    if 'Spread' in data.columns:
        data['Spread'] = data['Spread'].astype(str).str.replace('-', ',', regex=False)
        logging.info("Replaced dashes with commas in 'Spread' column.")
        print("Replaced dashes with commas in 'Spread' column.")

    # Save updated CSV
    try:
        data.to_csv(csv_path, index=False)
        logging.info("Updated tradeplan.csv saved.")
        print("Updated tradeplan.csv saved.")
    except Exception as e:
        logging.error(f"Error saving CSV: {str(e)}")
        print(f"Error saving CSV: {str(e)}")
        conn.close()
        sys.exit(1)

    # Deactivate all schedules before we re-activate them from CSV
    try:
        conn.execute("BEGIN TRANSACTION")
        conn.execute("UPDATE ScheduleMaster SET IsActive=0")
        conn.commit()
        logging.info("All schedules deactivated.")
        print("All schedules deactivated.")
    except sqlite3.Error as e:
        logging.error(f"Error deactivating schedules: {str(e)}")
        print(f"Error deactivating schedules: {str(e)}")
        conn.rollback()
        conn.close()
        sys.exit(1)

    # Create or ensure trade conditions exist (no transaction here)
    trade_condition_ids = create_trade_conditions(conn)

    # Now process the CSV updates (this function does BEGIN/COMMIT)
    process_tradeplan(conn, data, trade_condition_ids)

    # Close DB
    try:
        conn.close()
        logging.info("Database connection closed.")
    except sqlite3.Error as e:
        logging.error(f"Error closing DB: {str(e)}")

    # Remove the unzipped backup
    try:
        os.remove(backup_filepath)
        logging.info(f"Removed unzipped backup file: {backup_filepath}")
        print(f"Removed unzipped backup file: {backup_filepath}")
    except FileNotFoundError:
        logging.warning("Backup file not found. Skipping deletion.")
        print("Warning: Backup file not found. Skipping deletion.")

    logging.info("Script executed successfully.")
    print("Script executed successfully.\n")
    print("Final tradeplan.csv content:")
    print(data.to_csv(index=False))


if __name__ == "__main__":
    main()
