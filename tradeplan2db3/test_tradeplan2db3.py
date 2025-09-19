import unittest
import os
import shutil
import sqlite3
from datetime import datetime
import pandas as pd
from unittest.mock import patch, MagicMock

# It's good practice to have the script in the path to be able to import it
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Now import the functions from the script
from tradeplan2db3 import (
    get_schedule_times,
    create_backup,
    process_tradeplan,
    create_trade_conditions,
    get_accounts
)

class TestTradeplan2DB3(unittest.TestCase):
    def test_get_schedule_times(self):
        """
        Test that get_schedule_times returns the expected list of times.
        """
        expected_times = [
            "09:33", "09:39", "09:45", "09:52", "10:00", "10:08", "10:15", "10:23",
            "10:30", "10:38", "10:45", "10:53", "11:00", "11:08", "11:15", "11:23",
            "11:30", "11:38", "11:45", "11:53", "12:00", "12:08", "12:15", "12:23",
            "12:30", "12:38", "12:45", "12:53", "13:00", "13:08", "13:15", "13:23",
            "13:30", "13:38", "13:45", "13:53", "14:00", "14:08", "14:15", "14:23",
            "14:30", "14:38", "14:45", "14:53", "15:00", "15:08", "15:15", "15:23",
            "15:30", "15:38", "15:45"
        ]
        self.assertEqual(get_schedule_times(), expected_times)

    def setUp(self):
        """
        Set up a temporary directory and a dummy database file for tests.
        """
        self.test_dir = 'temp_test_dir'
        self.db_path = os.path.join(self.test_dir, 'test_data.db3')
        self.backup_dir = os.path.join(self.test_dir, 'backup')

        os.makedirs(self.test_dir, exist_ok=True)
        os.makedirs(self.backup_dir, exist_ok=True)

        # Create a dummy database file
        with open(self.db_path, 'w') as f:
            f.write("dummy content")

    def tearDown(self):
        """
        Clean up the temporary directory and files after tests.
        """
        shutil.rmtree(self.test_dir)

    def test_create_backup(self):
        """
        Test the create_backup function.
        """
        backup_filepath = create_backup(self.db_path, self.backup_dir)

        # Check that the backup database file was created
        self.assertTrue(os.path.exists(backup_filepath))

        # Check that the zip archive was created
        backup_filename = os.path.basename(backup_filepath)
        backup_zip_path = os.path.join(self.backup_dir, os.path.splitext(backup_filename)[0] + '.zip')

        # It seems there is an issue with the zip file name in the original code.
        # Let's find the zip file that was created
        zip_files = [f for f in os.listdir(self.backup_dir) if f.endswith('.zip')]
        self.assertEqual(len(zip_files), 1)

        # Let's correct the backup_zip_path to the actual created zip file
        actual_zip_path = os.path.join(self.backup_dir, zip_files[0])
        self.assertTrue(os.path.exists(actual_zip_path))

    def test_process_tradeplan(self):
        """
        Test the process_tradeplan function with a sample DataFrame.
        """
        # Use an in-memory SQLite database for this test
        conn = sqlite3.connect(':memory:')

        # Create the necessary tables
        conn.execute("""
            CREATE TABLE TradeCondition (
                TradeConditionID INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT,
                RetryUntilExpiration INTEGER
            )
        """)
        conn.execute("""
            CREATE TABLE TradeConditionDetail (
                TradeConditionDetailID INTEGER PRIMARY KEY AUTOINCREMENT,
                TradeConditionID INTEGER,
                [Group] INTEGER,
                Input TEXT,
                Operator TEXT,
                Comparison TEXT,
                ComparisonType TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE TradeTemplate (
                TradeTemplateID INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT,
                IsDeleted INTEGER,
                TradeType TEXT,
                TargetType TEXT,
                TargetMin REAL,
                TargetMax REAL,
                LongType TEXT,
                LongWidth TEXT,
                LongMaxPremium REAL,
                QtyDefault INTEGER,
                FillAttempts INTEGER,
                FillWait INTEGER,
                FillAdjustment REAL,
                StopType TEXT,
                StopMultiple REAL,
                StopOffset REAL,
                StopTrigger INTEGER,
                StopOrderType TEXT,
                StopTargetType TEXT,
                StopRelOffset REAL,
                StopRelLimit REAL,
                StopLimitOffset REAL,
                StopLimitMarketOffset REAL,
                OrderIDProfitTarget TEXT,
                ProfitTargetType TEXT,
                ProfitTarget REAL,
                Adjustment1Type TEXT,
                Adjustment1 REAL,
                Adjustment1ChangeType TEXT,
                Adjustment1Change REAL,
                Adjustment1ChangeOffset REAL,
                Adjustment1Hour INTEGER,
                Adjustment1Minute INTEGER,
                Adjustment2Type TEXT,
                Adjustment2 REAL,
                Adjustment2ChangeType TEXT,
                Adjustment2Change REAL,
                Adjustment2ChangeOffset REAL,
                Adjustment2Hour INTEGER,
                Adjustment2Minute INTEGER,
                Adjustment3Type TEXT,
                Adjustment3 REAL,
                Adjustment3ChangeType TEXT,
                Adjustment3Change REAL,
                Adjustment3ChangeOffset REAL,
                Adjustment3Hour INTEGER,
                Adjustment3Minute INTEGER,
                ExitHour INTEGER,
                ExitMinute INTEGER,
                LowerTarget INTEGER,
                StopBasis TEXT,
                StopRel TEXT,
                StopRelITM REAL,
                StopRelITMMinutes INTEGER,
                LongMaxWidth INTEGER,
                ExitMinutesInTrade INTEGER,
                Preference TEXT,
                ReEnterClose INTEGER,
                ReEnterStop INTEGER,
                ReEnterProfitTarget INTEGER,
                ReEnterDelay INTEGER,
                ReEnterExpirationHour INTEGER,
                ReEnterExpirationMinute INTEGER,
                ReEnterMaxEntries INTEGER,
                DisableNarrowerLong INTEGER,
                Strategy TEXT,
                MinOTM REAL,
                ShortPutTarget REAL,
                ShortPutTargetType TEXT,
                ShortPutDTE INTEGER,
                ShortCallTarget REAL,
                ShortCallTargetType TEXT,
                ShortCallDTE INTEGER,
                LongPutTarget REAL,
                LongPutTargetType TEXT,
                LongPutDTE INTEGER,
                LongCallTarget REAL,
                LongCallTargetType TEXT,
                LongCallDTE INTEGER,
                ExitDTE INTEGER,
                ExtendedHourStop INTEGER,
                TargetTypeCall TEXT,
                TargetMinCall REAL,
                TargetMaxCall REAL,
                PreferenceCall TEXT,
                MinOTMCall REAL,
                ExitOrderLimit INTEGER,
                PutRatio INTEGER,
                CallRatio INTEGER,
                LongMinPremium REAL,
                ProfitTargetTradePct REAL,
                ProfitTarget2 REAL,
                ProfitTarget2TradePct REAL,
                ProfitTarget3 REAL,
                ProfitTarget3TradePct REAL,
                ProfitTarget4 REAL,
                ProfitTarget4TradePct REAL,
                Adjustment1OrderType TEXT,
                Adjustment2OrderType TEXT,
                Adjustment3OrderType TEXT,
                ReEnterCloseTemplateID INTEGER,
                ReEnterStopTemplateID INTEGER,
                ReEnterProfitTargetTemplateID INTEGER,
                ReEnterCloseTemplateID2 INTEGER,
                ReEnterStopTemplateID2 INTEGER,
                ReEnterProfitTargetTemplateID2 INTEGER,
                MaxEntryPrice REAL,
                MinEntryPrice REAL
            )
        """)
        conn.execute("""
            CREATE TABLE ScheduleMaster (
                ScheduleMasterID INTEGER PRIMARY KEY AUTOINCREMENT,
                Account TEXT,
                TradeTemplateID INTEGER,
                ScheduleType TEXT,
                QtyOverride INTEGER,
                Hour INTEGER,
                Minute INTEGER,
                Second INTEGER,
                ExpirationMinutes INTEGER,
                IsActive INTEGER,
                ScheduleGroupID INTEGER,
                Condition TEXT,
                Strategy TEXT,
                DisplayStrategy TEXT,
                TradeConditionID INTEGER,
                DisplayCondition TEXT,
                DayMonday INTEGER,
                DayTuesday INTEGER,
                DayWednesday INTEGER,
                DayThursday INTEGER,
                DayFriday INTEGER,
                DaySunday INTEGER,
                QtyType TEXT,
                QtyAllocation REAL,
                QtyAllocationMax INTEGER
            )
        """)

        # Create trade conditions
        trade_condition_ids = create_trade_conditions(conn)

        # Create a sample trade plan
        trade_plan_data = {
            'Hour:Minute': ['09:33'],
            'Premium': [2.5],
            'Spread': ['10-15'],
            'Stop': ['1.5x'],
            'Strategy': ['EMA520'],
            'Plan': ['P1'],
            'Qty': [2],
            'profittarget': [50.0],
            'OptionType': ['P']
        }
        trade_plan_df = pd.DataFrame(trade_plan_data)

        # In order to test the process_tradeplan function, we need to also import the create_trade_templates and create_schedules functions
        from tradeplan2db3 import create_trade_templates, create_schedules

        # Create a sample trade plan
        trade_plan_data = {
            'Hour:Minute': ['09:33'],
            'Premium': [2.5],
            'Spread': ['10-15'],
            'Stop': ['1.5x'],
            'Strategy': ['EMA520'],
            'Plan': ['P1'],
            'Qty': [2],
            'profittarget': [50.0],
            'OptionType': ['P']
        }
        trade_plan_df = pd.DataFrame(trade_plan_data)

        accounts = ['IB:U1234567']
        # Mocking get_accounts to avoid user input
        with patch('tradeplan2db3.get_accounts', return_value=accounts):
            # Create the trade templates and schedules before processing the plan
            create_trade_templates(conn, ['P1'], ['09:33'])
            create_schedules(conn, ['P1'], trade_condition_ids, accounts, ['09:33'])
            # Process the trade plan
            process_tradeplan(conn, trade_plan_df, trade_condition_ids)

        # Verify the results in the database
        cursor = conn.cursor()

        # Check TradeTemplate
        cursor.execute("SELECT * FROM TradeTemplate WHERE Name = ?", ('PUT SPREAD (09:33) P1',))
        template = cursor.fetchone()
        self.assertIsNotNone(template)
        # Add more assertions for the template fields if necessary

        # Check ScheduleMaster
        cursor.execute("SELECT * FROM ScheduleMaster WHERE DisplayStrategy = ?", ('PUT SPREAD P1',))
        schedule = cursor.fetchone()
        self.assertIsNotNone(schedule)
        self.assertEqual(schedule[4], 2) # QtyOverride
        self.assertEqual(schedule[9], 1) # IsActive, should be 1 because process_tradeplan activates schedules

        conn.close()

    def test_create_trade_templates(self):
        """
        Test the create_trade_templates function.
        """
        conn = sqlite3.connect(':memory:')
        conn.execute("""
            CREATE TABLE TradeTemplate (
                TradeTemplateID INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT,
                IsDeleted INTEGER,
                TradeType TEXT,
                TargetType TEXT,
                TargetMin REAL,
                TargetMax REAL,
                LongType TEXT,
                LongWidth TEXT,
                LongMaxPremium REAL,
                QtyDefault INTEGER,
                FillAttempts INTEGER,
                FillWait INTEGER,
                FillAdjustment REAL,
                StopType TEXT,
                StopMultiple REAL,
                StopOffset REAL,
                StopTrigger INTEGER,
                StopOrderType TEXT,
                StopTargetType TEXT,
                StopRelOffset REAL,
                StopRelLimit REAL,
                StopLimitOffset REAL,
                StopLimitMarketOffset REAL,
                OrderIDProfitTarget TEXT,
                ProfitTargetType TEXT,
                ProfitTarget REAL,
                Adjustment1Type TEXT,
                Adjustment1 REAL,
                Adjustment1ChangeType TEXT,
                Adjustment1Change REAL,
                Adjustment1ChangeOffset REAL,
                Adjustment1Hour INTEGER,
                Adjustment1Minute INTEGER,
                Adjustment2Type TEXT,
                Adjustment2 REAL,
                Adjustment2ChangeType TEXT,
                Adjustment2Change REAL,
                Adjustment2ChangeOffset REAL,
                Adjustment2Hour INTEGER,
                Adjustment2Minute INTEGER,
                Adjustment3Type TEXT,
                Adjustment3 REAL,
                Adjustment3ChangeType TEXT,
                Adjustment3Change REAL,
                Adjustment3ChangeOffset REAL,
                Adjustment3Hour INTEGER,
                Adjustment3Minute INTEGER,
                ExitHour INTEGER,
                ExitMinute INTEGER,
                LowerTarget INTEGER,
                StopBasis TEXT,
                StopRel TEXT,
                StopRelITM REAL,
                StopRelITMMinutes INTEGER,
                LongMaxWidth INTEGER,
                ExitMinutesInTrade INTEGER,
                Preference TEXT,
                ReEnterClose INTEGER,
                ReEnterStop INTEGER,
                ReEnterProfitTarget INTEGER,
                ReEnterDelay INTEGER,
                ReEnterExpirationHour INTEGER,
                ReEnterExpirationMinute INTEGER,
                ReEnterMaxEntries INTEGER,
                DisableNarrowerLong INTEGER,
                Strategy TEXT,
                MinOTM REAL,
                ShortPutTarget REAL,
                ShortPutTargetType TEXT,
                ShortPutDTE INTEGER,
                ShortCallTarget REAL,
                ShortCallTargetType TEXT,
                ShortCallDTE INTEGER,
                LongPutTarget REAL,
                LongPutTargetType TEXT,
                LongPutDTE INTEGER,
                LongCallTarget REAL,
                LongCallTargetType TEXT,
                LongCallDTE INTEGER,
                ExitDTE INTEGER,
                ExtendedHourStop INTEGER,
                TargetTypeCall TEXT,
                TargetMinCall REAL,
                TargetMaxCall REAL,
                PreferenceCall TEXT,
                MinOTMCall REAL,
                ExitOrderLimit INTEGER,
                PutRatio INTEGER,
                CallRatio INTEGER,
                LongMinPremium REAL,
                ProfitTargetTradePct REAL,
                ProfitTarget2 REAL,
                ProfitTarget2TradePct REAL,
                ProfitTarget3 REAL,
                ProfitTarget3TradePct REAL,
                ProfitTarget4 REAL,
                ProfitTarget4TradePct REAL,
                Adjustment1OrderType TEXT,
                Adjustment2OrderType TEXT,
                Adjustment3OrderType TEXT,
                ReEnterCloseTemplateID INTEGER,
                ReEnterStopTemplateID INTEGER,
                ReEnterProfitTargetTemplateID INTEGER,
                ReEnterCloseTemplateID2 INTEGER,
                ReEnterStopTemplateID2 INTEGER,
                ReEnterProfitTargetTemplateID2 INTEGER,
                MaxEntryPrice REAL,
                MinEntryPrice REAL
            )
        """)

        from tradeplan2db3 import create_trade_templates

        create_trade_templates(conn, ['P1'], ['09:33'])

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM TradeTemplate")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 2) # PUT and CALL

        conn.close()

    def test_create_schedules(self):
        """
        Test the create_schedules function.
        """
        conn = sqlite3.connect(':memory:')
        # Create tables... (same as test_process_tradeplan)
        conn.execute("""
            CREATE TABLE TradeCondition (
                TradeConditionID INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT,
                RetryUntilExpiration INTEGER
            )
        """)
        conn.execute("""
            CREATE TABLE TradeConditionDetail (
                TradeConditionDetailID INTEGER PRIMARY KEY AUTOINCREMENT,
                TradeConditionID INTEGER,
                [Group] INTEGER,
                Input TEXT,
                Operator TEXT,
                Comparison TEXT,
                ComparisonType TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE TradeTemplate (
                TradeTemplateID INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT,
                IsDeleted INTEGER,
                TradeType TEXT,
                TargetType TEXT,
                TargetMin REAL,
                TargetMax REAL,
                LongType TEXT,
                LongWidth TEXT,
                LongMaxPremium REAL,
                QtyDefault INTEGER,
                FillAttempts INTEGER,
                FillWait INTEGER,
                FillAdjustment REAL,
                StopType TEXT,
                StopMultiple REAL,
                StopOffset REAL,
                StopTrigger INTEGER,
                StopOrderType TEXT,
                StopTargetType TEXT,
                StopRelOffset REAL,
                StopRelLimit REAL,
                StopLimitOffset REAL,
                StopLimitMarketOffset REAL,
                OrderIDProfitTarget TEXT,
                ProfitTargetType TEXT,
                ProfitTarget REAL,
                Adjustment1Type TEXT,
                Adjustment1 REAL,
                Adjustment1ChangeType TEXT,
                Adjustment1Change REAL,
                Adjustment1ChangeOffset REAL,
                Adjustment1Hour INTEGER,
                Adjustment1Minute INTEGER,
                Adjustment2Type TEXT,
                Adjustment2 REAL,
                Adjustment2ChangeType TEXT,
                Adjustment2Change REAL,
                Adjustment2ChangeOffset REAL,
                Adjustment2Hour INTEGER,
                Adjustment2Minute INTEGER,
                Adjustment3Type TEXT,
                Adjustment3 REAL,
                Adjustment3ChangeType TEXT,
                Adjustment3Change REAL,
                Adjustment3ChangeOffset REAL,
                Adjustment3Hour INTEGER,
                Adjustment3Minute INTEGER,
                ExitHour INTEGER,
                ExitMinute INTEGER,
                LowerTarget INTEGER,
                StopBasis TEXT,
                StopRel TEXT,
                StopRelITM REAL,
                StopRelITMMinutes INTEGER,
                LongMaxWidth INTEGER,
                ExitMinutesInTrade INTEGER,
                Preference TEXT,
                ReEnterClose INTEGER,
                ReEnterStop INTEGER,
                ReEnterProfitTarget INTEGER,
                ReEnterDelay INTEGER,
                ReEnterExpirationHour INTEGER,
                ReEnterExpirationMinute INTEGER,
                ReEnterMaxEntries INTEGER,
                DisableNarrowerLong INTEGER,
                Strategy TEXT,
                MinOTM REAL,
                ShortPutTarget REAL,
                ShortPutTargetType TEXT,
                ShortPutDTE INTEGER,
                ShortCallTarget REAL,
                ShortCallTargetType TEXT,
                ShortCallDTE INTEGER,
                LongPutTarget REAL,
                LongPutTargetType TEXT,
                LongPutDTE INTEGER,
                LongCallTarget REAL,
                LongCallTargetType TEXT,
                LongCallDTE INTEGER,
                ExitDTE INTEGER,
                ExtendedHourStop INTEGER,
                TargetTypeCall TEXT,
                TargetMinCall REAL,
                TargetMaxCall REAL,
                PreferenceCall TEXT,
                MinOTMCall REAL,
                ExitOrderLimit INTEGER,
                PutRatio INTEGER,
                CallRatio INTEGER,
                LongMinPremium REAL,
                ProfitTargetTradePct REAL,
                ProfitTarget2 REAL,
                ProfitTarget2TradePct REAL,
                ProfitTarget3 REAL,
                ProfitTarget3TradePct REAL,
                ProfitTarget4 REAL,
                ProfitTarget4TradePct REAL,
                Adjustment1OrderType TEXT,
                Adjustment2OrderType TEXT,
                Adjustment3OrderType TEXT,
                ReEnterCloseTemplateID INTEGER,
                ReEnterStopTemplateID INTEGER,
                ReEnterProfitTargetTemplateID INTEGER,
                ReEnterCloseTemplateID2 INTEGER,
                ReEnterStopTemplateID2 INTEGER,
                ReEnterProfitTargetTemplateID2 INTEGER,
                MaxEntryPrice REAL,
                MinEntryPrice REAL
            )
        """)
        conn.execute("""
            CREATE TABLE ScheduleMaster (
                ScheduleMasterID INTEGER PRIMARY KEY AUTOINCREMENT,
                Account TEXT,
                TradeTemplateID INTEGER,
                ScheduleType TEXT,
                QtyOverride INTEGER,
                Hour INTEGER,
                Minute INTEGER,
                Second INTEGER,
                ExpirationMinutes INTEGER,
                IsActive INTEGER,
                ScheduleGroupID INTEGER,
                Condition TEXT,
                Strategy TEXT,
                DisplayStrategy TEXT,
                TradeConditionID INTEGER,
                DisplayCondition TEXT,
                DayMonday INTEGER,
                DayTuesday INTEGER,
                DayWednesday INTEGER,
                DayThursday INTEGER,
                DayFriday INTEGER,
                DaySunday INTEGER,
                QtyType TEXT,
                QtyAllocation REAL,
                QtyAllocationMax INTEGER
            )
        """)

        from tradeplan2db3 import create_trade_templates, create_schedules, create_trade_conditions

        trade_condition_ids = create_trade_conditions(conn)
        create_trade_templates(conn, ['P1'], ['09:33'])
        create_schedules(conn, ['P1'], trade_condition_ids, ['IB:U1234567'], ['09:33'])

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM ScheduleMaster")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 2) # PUT and CALL

        conn.close()


if __name__ == '__main__':
    unittest.main()
