import pytest
import os
import sys
import shutil
import sqlite3
from datetime import datetime
import pandas as pd
from unittest.mock import patch

# Add the script's directory to the Python path to allow importing
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from tradeplan2db3 import (
    parse_arguments,
    get_accounts,
    get_schedule_times,
    create_backup,
    process_tradeplan,
    create_trade_conditions,
    create_trade_templates,
    create_schedules,
    initialize_database
)

def test_initialize_database_force(db_connection):
    """
    Test the initialize_database function with force=True.
    """
    # Add some dummy data to be deleted
    db_connection.execute("INSERT INTO TradeTemplate (Name) VALUES ('DUMMY_TEMPLATE')")
    db_connection.commit()

    initialize_database(db_connection, plan_count=2, force=True, accounts=['IB:U1234567'], times=['09:33'])

    cursor = db_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM TradeTemplate WHERE Name = 'DUMMY_TEMPLATE'")
    assert cursor.fetchone()[0] == 0

    cursor.execute("SELECT COUNT(*) FROM TradeTemplate")
    # 2 plans * 1 time * 2 types (put/call) = 4
    assert cursor.fetchone()[0] == 4

    cursor.execute("SELECT COUNT(*) FROM ScheduleMaster")
    assert cursor.fetchone()[0] == 4

def test_initialize_database_no_force(db_connection):
    """
    Test the initialize_database function with force=False (standard init).
    """
    # Add some dummy data that should NOT be deleted
    db_connection.execute("INSERT INTO TradeTemplate (Name) VALUES ('DUMMY_TEMPLATE')")
    db_connection.commit()

    initialize_database(db_connection, plan_count=1, force=False, accounts=['IB:U1234567'], times=['09:33'])

    cursor = db_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM TradeTemplate WHERE Name = 'DUMMY_TEMPLATE'")
    assert cursor.fetchone()[0] == 1

    cursor.execute("SELECT COUNT(*) FROM TradeTemplate")
    # 1 dummy + 1 plan * 1 time * 2 types = 3
    assert cursor.fetchone()[0] == 3

def test_get_accounts():
    """
    Test the get_accounts function for various user inputs.
    """
    # Test a single valid account entry
    with patch('builtins.input', side_effect=['IB:U1234567', 'n']):
        accounts = get_accounts()
        assert accounts == ['IB:U1234567']

    # Test multiple valid account entries with different formats
    with patch('builtins.input', side_effect=['U12345678', 'y', '1234567', 'n']):
        accounts = get_accounts()
        assert accounts == ['IB:U12345678', 'IB:U1234567']

    # Test invalid format followed by valid format
    with patch('builtins.input', side_effect=['invalid-account', 'IB:U87654321', 'n']):
        accounts = get_accounts()
        assert accounts == ['IB:U87654321']

    # Test empty input is rejected
    with patch('builtins.input', side_effect=['', 'IB:U87654321', 'n']):
        accounts = get_accounts()
        assert accounts == ['IB:U87654321']

def test_parse_arguments():
    """
    Test the parse_arguments function for all arguments.
    """
    with patch('sys.argv', ['tradeplan2db3.py', '--qty', '10']):
        args = parse_arguments()
        assert args.qty == 10

    with patch('sys.argv', ['tradeplan2db3.py', '--distribution']):
        args = parse_arguments()
        assert args.distribution is True

    with patch('sys.argv', ['tradeplan2db3.py', '--force-initialize', '5']):
        args = parse_arguments()
        assert args.force_initialize == 5

    with patch('sys.argv', ['tradeplan2db3.py', '--force-initialize']):
        args = parse_arguments()
        assert args.force_initialize == -1  # Sentinel value

    with patch('sys.argv', ['tradeplan2db3.py', '--initialize']):
        args = parse_arguments()
        assert args.initialize is True

    with patch('sys.argv', ['tradeplan2db3.py']):
        args = parse_arguments()
        assert args.qty is None
        assert args.distribution is False
        assert args.force_initialize is None
        assert args.initialize is False

def test_get_schedule_times():
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
    assert get_schedule_times() == expected_times

@pytest.fixture
def temp_backup_env(tmp_path):
    """
    Pytest fixture to create a temporary environment for backup testing.
    """
    db_dir = tmp_path / "db"
    db_dir.mkdir()
    db_path = db_dir / "test_data.db3"
    db_path.write_text("dummy content")

    backup_dir = tmp_path / "backup"
    backup_dir.mkdir()

    yield db_path, backup_dir

def test_create_backup(temp_backup_env):
    """
    Test the create_backup function.
    """
    db_path, backup_dir = temp_backup_env

    backup_filepath = create_backup(str(db_path), str(backup_dir))

    # Check that the backup database file was created
    assert os.path.exists(backup_filepath)

    # Check that the zip archive was created
    zip_files = [f for f in os.listdir(backup_dir) if f.endswith('.zip')]
    assert len(zip_files) == 1

    actual_zip_path = os.path.join(backup_dir, zip_files[0])
    assert os.path.exists(actual_zip_path)

def test_create_backup_file_not_found(tmp_path, caplog):
    """
    Test that create_backup handles a non-existent source database file.
    """
    non_existent_db = tmp_path / "non_existent.db3"
    backup_dir = tmp_path / "backup"
    backup_dir.mkdir()

    with patch('sys.exit') as mock_exit:
        create_backup(str(non_existent_db), str(backup_dir))
        mock_exit.assert_called_once_with(1)

    assert "Database file not found" in caplog.text

@pytest.fixture
def db_connection():
    """
    Pytest fixture to set up an in-memory SQLite database with the necessary schema.
    """
    conn = sqlite3.connect(':memory:')
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
    yield conn
    conn.close()

def test_create_trade_templates(db_connection):
    """
    Test the create_trade_templates function.
    """
    create_trade_templates(db_connection, ['P1'], ['09:33'])

    cursor = db_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM TradeTemplate")
    count = cursor.fetchone()[0]
    assert count == 2 # PUT and CALL

def test_create_schedules(db_connection):
    """
    Test the create_schedules function.
    """
    trade_condition_ids = create_trade_conditions(db_connection)
    create_trade_templates(db_connection, ['P1'], ['09:33'])
    create_schedules(db_connection, ['P1'], trade_condition_ids, ['IB:U1234567'], ['09:33'])

    cursor = db_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM ScheduleMaster")
    count = cursor.fetchone()[0]
    assert count == 2 # PUT and CALL

@pytest.fixture
def trade_plan_fixture(db_connection):
    """
    Fixture to set up the database with initial data for process_tradeplan tests.
    """
    trade_condition_ids = create_trade_conditions(db_connection)
    create_trade_templates(db_connection, ['P1', 'P2'], ['09:33', '10:00'])
    accounts = ['IB:U1234567']
    create_schedules(db_connection, ['P1', 'P2'], trade_condition_ids, accounts, ['09:33', '10:00'])
    return db_connection, trade_condition_ids

def test_process_tradeplan_with_minpremium(trade_plan_fixture):
    """
    Test that process_tradeplan correctly processes the MinPremium column.
    """
    conn, trade_condition_ids = trade_plan_fixture
    trade_plan_df = pd.DataFrame({
        'Hour:Minute': ['09:33'],
        'Premium': [2.5],
        'MinPremium': [1.2],
        'Spread': ['10-15'],
        'Stop': ['1.5x'],
        'Strategy': ['EMA520'],
        'Plan': ['P1'],
        'Qty': [2],
        'profittarget': [50.0],
        'OptionType': ['P']
    })

    process_tradeplan(conn, trade_plan_df, trade_condition_ids)

    cursor = conn.cursor()

    # Verification for the PUT template (P1 at 09:33)
    cursor.execute("SELECT LongMinPremium FROM TradeTemplate WHERE Name = ?", ('PUT SPREAD (09:33) P1',))
    put_template_res = cursor.fetchone()
    assert put_template_res[0] == 1.2


def test_process_tradeplan_updates_and_activates_schedules(trade_plan_fixture):
    """
    Test that process_tradeplan correctly updates templates and activates schedules
    based on a comprehensive CSV including the new MinPremium field.
    """
    conn, trade_condition_ids = trade_plan_fixture
    trade_plan_df = pd.DataFrame({
        'Hour:Minute': ['09:33', '10:00'],
        'Premium': [2.5, 3.0],
        'MinPremium': [1.0, 1.5],
        'Spread': ['10-15', '20-25'],
        'Stop': ['1.5x', '2x'],
        'Strategy': ['EMA520', 'EMA540'],
        'Plan': ['P1', 'P2'],
        'Qty': [2, 4],
        'profittarget': [50.0, 70.0],
        'OptionType': ['P', 'C']
    })

    process_tradeplan(conn, trade_plan_df, trade_condition_ids)

    cursor = conn.cursor()

    # Verification for the PUT template (P1 at 09:33)
    cursor.execute("SELECT TargetMax, LongMinPremium, LongWidth, StopMultiple, ProfitTarget FROM TradeTemplate WHERE Name = ?", ('PUT SPREAD (09:33) P1',))
    put_template_res = cursor.fetchone()
    assert put_template_res[0] == 2.5
    assert put_template_res[1] == 1.0
    assert put_template_res[2] == '10-15'
    assert put_template_res[3] == 1.5
    assert put_template_res[4] == 50.0

    # Verification for the CALL template (P2 at 10:00)
    cursor.execute("SELECT TargetMaxCall, LongMinPremium, LongWidth, StopMultiple, ProfitTarget FROM TradeTemplate WHERE Name = ?", ('CALL SPREAD (10:00) P2',))
    call_template_res = cursor.fetchone()
    assert call_template_res[0] == 3.0
    assert call_template_res[1] == 1.5
    assert call_template_res[2] == '20-25'
    assert call_template_res[3] == 2.0
    assert call_template_res[4] == 70.0

    # Verification for the PUT schedule activation
    cursor.execute("SELECT QtyOverride, IsActive FROM TradeTemplate tt JOIN ScheduleMaster sm ON tt.TradeTemplateID = sm.TradeTemplateID WHERE tt.Name = ?", ('PUT SPREAD (09:33) P1',))
    put_schedule_res = cursor.fetchone()
    assert put_schedule_res[0] == 2
    assert put_schedule_res[1] == 1

    # Verification for the CALL schedule activation
    cursor.execute("SELECT QtyOverride, IsActive FROM TradeTemplate tt JOIN ScheduleMaster sm ON tt.TradeTemplateID = sm.TradeTemplateID WHERE tt.Name = ?", ('CALL SPREAD (10:00) P2',))
    call_schedule_res = cursor.fetchone()
    assert call_schedule_res[0] == 4
    assert call_schedule_res[1] == 1

def test_process_tradeplan_no_option_type(trade_plan_fixture):
    """
    Test process_tradeplan when OptionType is not specified (should update both PUT and CALL).
    """
    conn, trade_condition_ids = trade_plan_fixture
    trade_plan_df = pd.DataFrame({
        'Hour:Minute': ['10:00'], 'Premium': [3.0], 'Spread': ['20-25'], 'Stop': ['2x'],
        'Strategy': ['EMA540'], 'Plan': ['P2'], 'Qty': [4], 'profittarget': [70.0]
    })
    process_tradeplan(conn, trade_plan_df, trade_condition_ids)

    cursor = conn.cursor()
    # Check PUT
    cursor.execute("SELECT ProfitTarget, QtyOverride, IsActive FROM TradeTemplate tt JOIN ScheduleMaster sm ON tt.TradeTemplateID = sm.TradeTemplateID WHERE tt.Name = ?", ('PUT SPREAD (10:00) P2',))
    put_res = cursor.fetchone()
    assert put_res[0] == 70.0
    assert put_res[1] == 4
    assert put_res[2] == 1

    # Check CALL
    cursor.execute("SELECT ProfitTarget, QtyOverride, IsActive FROM TradeTemplate tt JOIN ScheduleMaster sm ON tt.TradeTemplateID = sm.TradeTemplateID WHERE tt.Name = ?", ('CALL SPREAD (10:00) P2',))
    call_res = cursor.fetchone()
    assert call_res[0] == 70.0
    assert call_res[1] == 4
    assert call_res[2] == 1

def test_process_tradeplan_profittarget_100(trade_plan_fixture):
    """
    Test that a profittarget of 100 is treated as None (no profit target).
    """
    conn, trade_condition_ids = trade_plan_fixture
    trade_plan_df = pd.DataFrame({
        'Hour:Minute': ['09:33'], 'Premium': [2.5], 'Spread': ['10-15'], 'Stop': ['1.5x'],
        'Strategy': ['EMA520'], 'Plan': ['P1'], 'Qty': [2], 'profittarget': [100.0], 'OptionType': ['P']
    })
    process_tradeplan(conn, trade_plan_df, trade_condition_ids)

    cursor = conn.cursor()
    cursor.execute("SELECT ProfitTarget, OrderIDProfitTarget FROM TradeTemplate WHERE Name = ?", ('PUT SPREAD (09:33) P1',))
    res = cursor.fetchone()
    assert res[0] is None
    assert res[1] == "None"

def test_process_tradeplan_missing_plan(trade_plan_fixture):
    """
    Test that a missing 'Plan' column defaults to 'P1'.
    """
    conn, trade_condition_ids = trade_plan_fixture
    trade_plan_df = pd.DataFrame({
        'Hour:Minute': ['09:33'], 'Premium': [2.5], 'Spread': ['10-15'], 'Stop': ['1.5x'],
        'Strategy': ['EMA520'], 'Qty': [2], 'profittarget': [50.0], 'OptionType': ['P']
    })
    process_tradeplan(conn, trade_plan_df, trade_condition_ids)

    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM TradeTemplate WHERE Name = ?", ('PUT SPREAD (09:33) P1',))
    assert cursor.fetchone()[0] == 1

def test_process_tradeplan_invalid_strategy(trade_plan_fixture):
    """
    Test that an invalid 'Strategy' defaults to 'EMA520'.
    """
    conn, trade_condition_ids = trade_plan_fixture
    trade_plan_df = pd.DataFrame({
        'Hour:Minute': ['09:33'], 'Premium': [2.5], 'Spread': ['10-15'], 'Stop': ['1.5x'],
        'Strategy': ['INVALID'], 'Plan': ['P1'], 'Qty': [2], 'profittarget': [50.0], 'OptionType': ['P']
    })
    with pytest.raises(ValueError, match="Unsupported Strategy 'INVALID' in CSV."):
        process_tradeplan(conn, trade_plan_df, trade_condition_ids)

def test_process_tradeplan_stop_x(trade_plan_fixture):
    """
    Test that a 'Stop' value of 'x' is correctly parsed as 1.0.
    """
    conn, trade_condition_ids = trade_plan_fixture
    trade_plan_df = pd.DataFrame({
        'Hour:Minute': ['09:33'], 'Premium': [2.5], 'Spread': ['10-15'], 'Stop': ['x'],
        'Strategy': ['EMA520'], 'Plan': ['P1'], 'Qty': [2], 'profittarget': [50.0], 'OptionType': ['P']
    })
    process_tradeplan(conn, trade_plan_df, trade_condition_ids)

    cursor = conn.cursor()
    cursor.execute("SELECT StopMultiple FROM TradeTemplate WHERE Name = ?", ('PUT SPREAD (09:33) P1',))
    res = cursor.fetchone()
    assert res[0] == 1.0
