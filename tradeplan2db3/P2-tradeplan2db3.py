"""Updates the Trade Automation Toolbox database from a CSV trade plan.

>>> SPECIAL BUILD — paired with the P2 multi_day_return regime filter. <<<
This version implements SKIP DAY handling: when the upstream tradeplan engine
(strategy_v17.sh + generate_tradeplan.py --target-date) decides the regime
filter blocks the trading day, it writes an EMPTY tradeplan CSV. This script
then DELETES the plan's schedules so TAT fires nothing that day, instead of
leaving yesterday's plan in place. Required by P2 — without it, a skip day
would silently run yesterday's slots against today's market.

Reads tradeplan2db3.ini for configuration (account number, DB path, etc.),
reads a tradeplan CSV, and writes Trade Templates + Schedules into data.db3.

Key features:
- SKIP DAY support (P2-special): empty CSV → deactivate the plan's schedules.
- Timestamped backup before any mutation; aborts if backup fails.
- Template defaults aligned with TAT 4.4.81 GUI export (Adjustment/TSL slots
  default to export values; CSV tsl* columns are ignored).
- SQL-injection guard: column names validated against frozen allowlists.
- All errors logged to file AND console via dual-handler logging.
- Strategy lookup is data-driven; unknown strategies are a hard stop.

CSV column → DB field mapping:
    Premium     → TargetMaxCall (CALL) / TargetMax + TargetMaxCall (PUT)
    Spread      → LongWidthCall + LongMaxWidthCall (both sides) + LongMaxWidth (PUT only)
    Stop        → StopMultiple (strip 'x' suffix)
    Strategy    → EMA condition pair (see STRATEGY_MAP)
    profittarget→ (reserved; not yet wired)
    tsl{N}_*    → (ignored; per-plan TSL writes were removed 2026-05-10)

Long-leg cap (LongMaxPremium / LongMaxPremiumCall) pattern per reference exports:
    CALL: active Call → 1.0, inactive Put → null
    PUT:  both sides  → 1.0
"""

__version__ = "1.1.1-p2"
__updated__ = "2026-05-24"
__build__   = "SPECIAL — P2 regime-filter (multi_day_return) skip-day build"
__changelog__ = """
1.1.1-p2 — 2026-05-24  (skip-marker plan-name embedding)
  - Now recognises a SKIP-marker row inside the CSV (header + 1 row with
    "SKIP" in Hour:Minute, plan name in Plan column). The upstream engine
    emits this on regime-skip days so the plan name survives any filename
    change on the TAT side (which often renames the file to the INI's
    generic `tradeplan.csv` default and would otherwise lose the hint).
  - Truly-empty-CSV path (filename fallback) kept as legacy safety net.

1.1.0-p2 — 2026-05-24  (SPECIAL BUILD for P2 multi_day_return strategy)
  - SKIP DAY support: an empty CSV no longer aborts the run. Plan name is
    derived from the `--plan` CLI flag or the `tradeplan-<NAME>.csv` filename
    pattern produced by strategy_v17.sh; all ScheduleMaster rows for that plan
    are DELETEd, the transaction is committed, and the script exits 0.
  - New helpers: deactivate_plan(), derive_plan_name_from_csv_path()
  - New CLI flag: --plan <NAME> (override for skip-day plan lookup when the
    filename pattern doesn't apply)
  - Required by P2's `multi_day_return: window=2, min=-1.0` filter — without
    this, a skip day would silently leave yesterday's schedules active and
    TAT would fire stale slots.

1.0.x — original (no version tracking)
  - Per-CSV-row TradeTemplate + ScheduleMaster upsert
  - Timestamped backup before mutation
  - SQL-injection allowlist for columns and tables
  - WORKING-template-aligned defaults (TAT 4.4.81)
"""

import argparse
import configparser
import logging
import shutil
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd

# ── Paths ──────────────────────────────────────────────────────────────────────
_DIR         = Path(__file__).parent
INI_FILENAME = _DIR / "tradeplan2db3.ini"
LOG_FILENAME = _DIR / "tradeplan_update.log"

# ── SQL injection guards: only these identifiers may appear in dynamic SQL ─────
_ALLOWED_TABLES: frozenset[str] = frozenset({
    "TradeTemplate",
    "TradeCondition",
    "TradeConditionDetail",
    "ScheduleMaster",
})

_ALLOWED_TEMPLATE_COLS: frozenset[str] = frozenset({
    # Identity / type
    "Name", "Strategy", "TradeType", "UnderlyingSymbol", "IsDeleted",
    # Target / premium
    "TargetType", "TargetTypeCall", "TargetMin", "TargetMinCall",
    "TargetMax", "TargetMaxCall",
    "LongType", "LongTypeCall", "LongWidth", "LongWidthCall",
    "LongMaxPremium", "LongMaxPremiumCall", "LongMaxWidth", "LongMaxWidthCall",
    "LongMinPremium", "LongMinPremiumCall",
    "MinOTM", "MinOTMCall",
    "MaxEntryPrice", "MinEntryPrice",
    # Order fill / qty
    "QtyDefault", "FillAttempts", "FillWait", "FillAdjustment",
    "PutRatio", "CallRatio",
    # Stop
    "StopType", "StopMultiple", "StopOffset", "StopTrigger",
    "StopOrderType", "StopTargetType", "StopBasis", "StopRel",
    "StopRelOffset", "StopRelLimit", "StopLimitOffset", "StopLimitMarketOffset",
    "StopRelITM", "StopRelITMMinutes",
    # Profit target ladder
    "OrderIDProfitTarget", "ProfitTargetType", "ProfitTarget",
    "ProfitTargetTradePct",
    "ProfitTarget2", "ProfitTarget2TradePct",
    "ProfitTarget3", "ProfitTarget3TradePct",
    "ProfitTarget4", "ProfitTarget4TradePct",
    "ProfitTargetExpirationHour", "ProfitTargetExpirationMinute",
    # Adjustments / TSL
    "Adjustment1Type", "Adjustment1", "Adjustment1ChangeType", "Adjustment1Change",
    "Adjustment1ChangeOffset", "Adjustment1Hour", "Adjustment1Minute", "Adjustment1OrderType",
    "Adjustment2Type", "Adjustment2", "Adjustment2ChangeType", "Adjustment2Change",
    "Adjustment2ChangeOffset", "Adjustment2Hour", "Adjustment2Minute", "Adjustment2OrderType",
    "Adjustment3Type", "Adjustment3", "Adjustment3ChangeType", "Adjustment3Change",
    "Adjustment3ChangeOffset", "Adjustment3Hour", "Adjustment3Minute", "Adjustment3OrderType",
    # Exit / hours
    "ExitHour", "ExitMinute", "ExitMinutesInTrade", "ExitDTE",
    "ExitOrderLimit", "ExitConditionID",
    "ExtendedHourStop", "ExtendedHourPT",
    "LowerTarget", "Preference", "PreferenceCall",
    # Re-entry
    "ReEnterClose", "ReEnterStop", "ReEnterProfitTarget",
    "ReEnterCloseTemplateID", "ReEnterStopTemplateID", "ReEnterProfitTargetTemplateID",
    "ReEnterCloseTemplateID2", "ReEnterStopTemplateID2", "ReEnterProfitTargetTemplateID2",
    "ReEnterDelay", "ReEnterExpirationHour", "ReEnterExpirationMinute", "ReEnterMaxEntries",
    # Long-side behaviour
    "LongReuseOnly", "DisableNarrowerLong", "DisableNarrowerLongCall",
    "ExcessLongBehavior", "AllOrNone", "AllowStrikeConflict",
    # Custom-leg targets (Short/Long Put/Call)
    "ShortPutTarget", "ShortPutTargetType", "ShortPutDTE",
    "ShortCallTarget", "ShortCallTargetType", "ShortCallDTE",
    "LongPutTarget", "LongPutTargetType", "LongPutDTE",
    "LongCallTarget", "LongCallTargetType", "LongCallDTE",
    # Custom-leg slots (CLeg1..CLeg4)
    "CLeg1Action", "CLeg1PutCall", "CLeg1Ratio", "CLeg1Target", "CLeg1TargetType", "CLeg1DTE",
    "CLeg2Action", "CLeg2PutCall", "CLeg2Ratio", "CLeg2Target", "CLeg2TargetType", "CLeg2DTE",
    "CLeg3Action", "CLeg3PutCall", "CLeg3Ratio", "CLeg3Target", "CLeg3TargetType", "CLeg3DTE",
    "CLeg4Action", "CLeg4PutCall", "CLeg4Ratio", "CLeg4Target", "CLeg4TargetType", "CLeg4DTE",
    # Misc
    "MinEntrySLRatio", "MaxEntrySLRatio",
    "UpgradeFlag", "ForceCloseAllLegs", "IsDebit",
})

_ALLOWED_SCHEDULE_COLS: frozenset[str] = frozenset({
    "ScheduleType", "Account", "QtyType", "QtyOverride", "ExpirationMinutes",
    "IsActive", "DayMonday", "DayTuesday", "DayWednesday", "DayThursday", "DayFriday",
    "TradeTemplateID", "TradeTemplateIDFailure", "TradeConditionID",
    "DisplayTemplate", "DisplayStrategy", "DisplayCondition", "Strategy",
    "Hour", "Minute",
})

# ── Strategy → (short_ema_label, long_ema_label) ──────────────────────────────
STRATEGY_MAP: dict[str, tuple[str, str]] = {
    "ema540":  ("EMA5",  "EMA40"),
    "ema520":  ("EMA5",  "EMA20"),
    "ema2040": ("EMA20", "EMA40"),
}

# ── TAT field-value constants ─────────────────────────────────────────────────
# Single source of truth for stringly-typed TAT enums.
# 2026-05-26: Corrected against manually-verified TAT reference exports
# (*-correct.tat). CALL inactive side uses null (not a placeholder value).
# PUT mirrors both sides to Max Premium with cap 1.0.
_PREF_HIGHEST            = "Highest Premium/Delta"
_LONG_TYPE_MAX_PREMIUM   = "Max Premium"
_LONG_TYPE_WIDTH         = "Width"
_LONG_MAX_WIDTH          = 100.0  # static fallback default
_LONG_MAX_PREMIUM_ACTIVE = 1.0    # per-leg cap (both sides on PUT, Call-side on CALL)

# ── Base trade templates ───────────────────────────────────────────────────────
# Defaults aligned with TAT 4.4.81 reference exports
# (Export-CALL-05102026-0515.tat, Export-PUT-05102026-0515.tat).
# Per-plan SQL in update_template_values() may overwrite any of these.
# Keep the keys in sync with _ALLOWED_TEMPLATE_COLS — the import-time drift
# guard below this dict will raise on mismatch.
_TEMPLATE_COMMON: dict = {
    # Identity / type
    "UnderlyingSymbol": "SPX",
    "Strategy": "",
    "IsDeleted": 0,
    # Target / premium
    "TargetType": "Premium",
    "TargetTypeCall": "Premium",
    "TargetMin": 0.0,
    "TargetMinCall": 0.0,
    # CALL-default pattern (PUT base + per-plan SQL override the asymmetric
    # PUT side). WORKING template (2026-05-10): active=Call uses Max Premium,
    # inactive=Put stays Width. PUT template flips this in TEMPLATE_BASE_PUT.
    "LongType": _LONG_TYPE_WIDTH,
    "LongTypeCall": _LONG_TYPE_MAX_PREMIUM,
    # LongWidth/LongWidthCall are CSV-Spread-derived at sync time.
    # TAT reference: LongWidth is always empty; LongWidthCall carries the spread.
    "LongWidth": "",
    "LongWidthCall": "100",
    "LongMaxPremium": None,
    "LongMaxPremiumCall": None,
    "LongMaxWidth": _LONG_MAX_WIDTH,
    "LongMaxWidthCall": _LONG_MAX_WIDTH,
    "LongMinPremium": None,
    "LongMinPremiumCall": None,
    "MinOTM": 0.0,
    "MinOTMCall": 0.0,
    "MaxEntryPrice": 0.0,
    "MinEntryPrice": 0.0,
    # Order fill / qty
    "QtyDefault": 1,
    "FillAttempts": 5,
    "FillWait": 15,
    "FillAdjustment": 0.05,
    "PutRatio": 1,
    "CallRatio": 1,
    # Stop
    "StopType": "Vertical",
    "StopMultiple": 2.0,
    "StopOffset": 0.0,
    "StopTrigger": 8,
    "StopOrderType": "StopMarket",
    "StopTargetType": "Multiple",
    "StopBasis": "Exact Price",
    "StopRel": 0,
    "StopRelOffset": None,
    "StopRelLimit": None,
    "StopLimitOffset": None,
    "StopLimitMarketOffset": None,
    "StopRelITM": None,
    "StopRelITMMinutes": None,
    # Profit target ladder
    "OrderIDProfitTarget": None,
    "ProfitTargetType": "None",
    "ProfitTarget": None,
    "ProfitTargetTradePct": 100.0,
    "ProfitTarget2": None,
    "ProfitTarget2TradePct": 100.0,
    "ProfitTarget3": None,
    "ProfitTarget3TradePct": 100.0,
    "ProfitTarget4": None,
    "ProfitTarget4TradePct": 100.0,
    "ProfitTargetExpirationHour": 0,
    "ProfitTargetExpirationMinute": 0,
    # Adjustment / TSL slots — TAT reference exports use null for Change/Offset
    # when AdjustmentType is "None". Per-plan TSL overrides removed 2026-05-10.
    "Adjustment1Type": "None",  "Adjustment1": None,
    "Adjustment1ChangeType": "Stop Multiple", "Adjustment1Change": None,
    "Adjustment1ChangeOffset": None, "Adjustment1Hour": 0, "Adjustment1Minute": 0,
    "Adjustment1OrderType": "Same",
    "Adjustment2Type": "None",  "Adjustment2": None,
    "Adjustment2ChangeType": "Stop Multiple", "Adjustment2Change": None,
    "Adjustment2ChangeOffset": None, "Adjustment2Hour": 0, "Adjustment2Minute": 0,
    "Adjustment2OrderType": "Same",
    "Adjustment3Type": "None",  "Adjustment3": None,
    "Adjustment3ChangeType": "Stop Multiple", "Adjustment3Change": None,
    "Adjustment3ChangeOffset": None, "Adjustment3Hour": 0, "Adjustment3Minute": 0,
    "Adjustment3OrderType": "Same",
    # Exit / hours
    "ExitHour": 0,
    "ExitMinute": 0,
    "ExitMinutesInTrade": 0,
    "ExitDTE": 0,
    "ExitOrderLimit": 0,
    "ExitConditionID": 0,
    "ExtendedHourStop": 0,
    "ExtendedHourPT": 1,
    "LowerTarget": 0,
    "Preference": _PREF_HIGHEST,
    "PreferenceCall": _PREF_HIGHEST,
    # Re-entry
    "ReEnterClose": 0,
    "ReEnterStop": 0,
    "ReEnterProfitTarget": 0,
    "ReEnterCloseTemplateID": 0,
    "ReEnterStopTemplateID": 0,
    "ReEnterProfitTargetTemplateID": 0,
    "ReEnterCloseTemplateID2": 0,
    "ReEnterStopTemplateID2": 0,
    "ReEnterProfitTargetTemplateID2": 0,
    "ReEnterDelay": 0,
    "ReEnterExpirationHour": 0,
    "ReEnterExpirationMinute": 0,
    "ReEnterMaxEntries": 1,
    # Long-side behaviour
    "LongReuseOnly": 0,
    "DisableNarrowerLong": 0,
    "DisableNarrowerLongCall": 0,
    "ExcessLongBehavior": 0,
    "AllOrNone": 0,
    "AllowStrikeConflict": 0,
    # Custom-leg targets (unused for vertical spreads; kept for schema parity)
    "ShortPutTarget": 0.0, "ShortPutTargetType": None, "ShortPutDTE": 0,
    "ShortCallTarget": 0.0, "ShortCallTargetType": None, "ShortCallDTE": 0,
    "LongPutTarget": 0.0, "LongPutTargetType": None, "LongPutDTE": 0,
    "LongCallTarget": 0.0, "LongCallTargetType": None, "LongCallDTE": 0,
    # Custom-leg slots (unused; default per export)
    "CLeg1Action": None, "CLeg1PutCall": None, "CLeg1Ratio": 0,
    "CLeg1Target": 0.0, "CLeg1TargetType": None, "CLeg1DTE": 0,
    "CLeg2Action": None, "CLeg2PutCall": None, "CLeg2Ratio": 0,
    "CLeg2Target": 0.0, "CLeg2TargetType": None, "CLeg2DTE": 0,
    "CLeg3Action": None, "CLeg3PutCall": None, "CLeg3Ratio": 0,
    "CLeg3Target": 0.0, "CLeg3TargetType": None, "CLeg3DTE": 0,
    "CLeg4Action": None, "CLeg4PutCall": None, "CLeg4Ratio": 0,
    "CLeg4Target": 0.0, "CLeg4TargetType": None, "CLeg4DTE": 0,
    # Misc
    "MinEntrySLRatio": 0.0,
    "MaxEntrySLRatio": 0.0,
    "UpgradeFlag": 0.0,
    "ForceCloseAllLegs": 0,
    "IsDebit": 0,
}

# ── Drift guard ────────────────────────────────────────────────────────────────
# Any key in _TEMPLATE_COMMON that is NOT in _ALLOWED_TEMPLATE_COLS would be
# silently dropped by ensure_template_exists()'s allowlist filter. Fail loudly
# at import time so this can never happen by accident (TAT version upgrades).
_drift = set(_TEMPLATE_COMMON) - _ALLOWED_TEMPLATE_COLS
if _drift:
    raise RuntimeError(
        "Template default keys missing from _ALLOWED_TEMPLATE_COLS — "
        f"these would be silently dropped: {sorted(_drift)}"
    )

TEMPLATE_BASE_CALL: dict = {
    **_TEMPLATE_COMMON,
    "TradeType": "CallSpread",
    "TargetMax": 0.0,
    "TargetMaxCall": 5.0,
}

TEMPLATE_BASE_PUT: dict = {
    **_TEMPLATE_COMMON,
    "TradeType": "PutSpread",
    "TargetMax": 5.0,
    "TargetMaxCall": 5.0,
}


# ── Dataclass for schedule creation ───────────────────────────────────────────
@dataclass
class SchedulePlan:
    plan_name: str
    call_template_id: int
    put_template_id: int
    call_condition_id: int
    put_condition_id: int
    call_condition_name: str
    put_condition_name: str
    account_number: str
    qty_override: int | None = None


# ── Configuration ──────────────────────────────────────────────────────────────
def load_config() -> configparser.ConfigParser:
    """Loads tradeplan2db3.ini; aborts with a clear message if absent or invalid."""
    cfg = configparser.ConfigParser()
    if not INI_FILENAME.exists():
        sys.exit(
            f"ERROR: Configuration file not found: {INI_FILENAME}\n"
            "Create it from tradeplan2db3.ini.example before running."
        )
    cfg.read(INI_FILENAME)
    account = cfg.get("account", "number", fallback="").strip()
    if not account:
        sys.exit("ERROR: [account] number is missing or empty in tradeplan2db3.ini")
    return cfg


# ── Logging ────────────────────────────────────────────────────────────────────
def setup_logging() -> None:
    """Configures dual-output logging: file + console, same format."""
    fmt = "%(asctime)s - %(levelname)s - %(message)s"
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    fh = logging.FileHandler(LOG_FILENAME)
    fh.setFormatter(logging.Formatter(fmt))

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter(fmt))

    root.addHandler(fh)
    root.addHandler(ch)


# ── Argument parsing ───────────────────────────────────────────────────────────
def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync tradeplan CSV into TAT data.db3.")
    parser.add_argument(
        "--qty", type=int,
        help="Override contract quantity for all entry times (takes precedence over CSV).",
    )
    parser.add_argument(
        "--csv", default=None,
        help="Path to the tradeplan CSV (default: tradeplan.csv next to this script).",
    )
    parser.add_argument(
        "--plan", default=None,
        help="Plan name override for empty-CSV (skip-day) handling. If the "
             "CSV has zero rows the script can't read the Plan column, so it "
             "needs this (or a filename like tradeplan-<NAME>.csv) to know "
             "which schedules to deactivate.",
    )
    return parser.parse_args()


# ── Database helpers ───────────────────────────────────────────────────────────
def backup_database(db_path: Path) -> Path:
    """
    Creates a timestamped backup of the database.

    Returns the backup path on success.
    Raises RuntimeError on failure so the caller can abort safely.
    """
    if not db_path.exists():
        raise RuntimeError(f"Database file not found: {db_path}")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = db_path.with_name(f"{db_path.name}.{timestamp}.bak")
    try:
        shutil.copy2(db_path, backup_path)
        logging.info("Database backup created: %s", backup_path)
        return backup_path
    except OSError as exc:
        raise RuntimeError(f"Backup failed: {exc}") from exc


def get_table_columns(cursor: sqlite3.Cursor, table_name: str) -> list[str]:
    """
    Returns column names for *table_name*.

    Raises ValueError if the table name is not in the allowlist (injection guard).
    """
    if table_name not in _ALLOWED_TABLES:
        raise ValueError(f"Disallowed table name: {table_name!r}")
    cursor.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cursor.fetchall()]


# ── Condition management ───────────────────────────────────────────────────────
def ensure_condition_exists(
    cursor: sqlite3.Cursor,
    condition_name: str,
    input_val: str,
    comparison_val: str,
    operator: Literal["<", ">"],
) -> int:
    """Returns the ID of the named condition, creating it if necessary."""
    logging.info("Checking for Condition '%s'...", condition_name)
    cursor.execute(
        "SELECT TradeConditionID FROM TradeCondition WHERE Name = ?",
        (condition_name,),
    )
    row = cursor.fetchone()
    if row:
        return int(row[0])

    logging.info("  -> Creating Condition '%s'...", condition_name)
    cursor.execute(
        "INSERT INTO TradeCondition (Name, RetryUntilExpiration) VALUES (?, 0)",
        (condition_name,),
    )
    condition_id = cursor.lastrowid
    cursor.execute(
        """
        INSERT INTO TradeConditionDetail
            (TradeConditionID, "Group", Input, Operator, Comparison,
             ComparisonType, InputDisplay, ComparisonDisplay)
        VALUES (?, 1, ?, ?, ?, 'Input', ?, ?)
        """,
        (condition_id, input_val, operator, comparison_val, input_val, comparison_val),
    )
    return int(condition_id)


# ── Template management ────────────────────────────────────────────────────────
def ensure_template_exists(
    cursor: sqlite3.Cursor,
    template_def: dict,
    template_name: str,
) -> int:
    """Upserts the named template. Returns its TradeTemplateID."""
    cursor.execute(
        "SELECT TradeTemplateID FROM TradeTemplate WHERE Name = ?",
        (template_name,),
    )
    row = cursor.fetchone()

    data = {**template_def, "Name": template_name, "Strategy": template_name}
    live_cols = set(get_table_columns(cursor, "TradeTemplate")) - {"TradeTemplateID"}
    clean = {k: v for k, v in data.items() if k in live_cols and k in _ALLOWED_TEMPLATE_COLS}

    if row:
        logging.info("  -> Template '%s' exists. Repairing/Updating base structure...", template_name)
        set_clause = ", ".join(f"{col} = ?" for col in clean)
        cursor.execute(
            f"UPDATE TradeTemplate SET {set_clause} WHERE Name = ?",
            [*clean.values(), template_name],
        )
        return int(row[0])

    logging.info("  -> Creating Template '%s'...", template_name)
    cols = ", ".join(clean)
    placeholders = ", ".join("?" * len(clean))
    cursor.execute(
        f"INSERT INTO TradeTemplate ({cols}) VALUES ({placeholders})",
        list(clean.values()),
    )
    return int(cursor.lastrowid)


def warn_inconsistent_rows(df: "pd.DataFrame") -> None:
    """
    Warns if columns applied template-wide vary across CSV rows.

    All schedules for a plan share ONE CALL + ONE PUT template, so only the
    first row's Premium/Spread/Stop/Strategy/Plan actually reach the DB.
    Without this warning, a user editing rows 2..N (and forgetting row 1)
    silently gets stale values from row 1 written to all schedules.
    """
    template_wide_cols = ("Premium", "Spread", "Stop", "Strategy", "Plan")
    for col in template_wide_cols:
        if col not in df.columns:
            continue
        unique_vals = df[col].astype(str).str.strip().unique().tolist()
        if len(unique_vals) > 1:
            logging.warning(
                "CSV column %r has inconsistent values across rows: %s. "
                "Only the FIRST row's value (%r) is applied template-wide; "
                "rows 2..N are ignored for this field.",
                col, unique_vals, unique_vals[0],
            )


def update_template_values(
    cursor: sqlite3.Cursor,
    first_row: "pd.Series",
    plan_name: str,
) -> None:
    """
    Writes per-plan CSV values onto the CALL and PUT templates.

    CSV → DB mapping:
        Premium ("5.0")   → TargetMaxCall (CALL) / TargetMax+TargetMaxCall (PUT)
        Spread  ("100")   → LongWidthCall, LongMaxWidth (PUT), LongMaxWidthCall
        Stop    ("1.75x") → StopMultiple

    Template pattern (verified 2026-05-26 against manually-created TAT exports):
        CALL: active=Call side has values; inactive=Put side nulled/empty.
              LongTypeCall='Max Premium', LongType='Width',
              LongMaxPremiumCall=1.0, LongMaxPremium=null,
              LongMaxWidthCall=spread, LongMaxWidth=null,
              LongWidth='', LongWidthCall=spread_str,
              Preference=PreferenceCall='Highest Premium/Delta'
        PUT:  both sides 'Max Premium' with cap 1.0; both TargetMax fields set.
              LongType=LongTypeCall='Max Premium',
              LongMaxPremium=LongMaxPremiumCall=1.0,
              LongMaxWidth=LongMaxWidthCall=spread,
              LongWidth='', LongWidthCall=spread_str,
              Preference=PreferenceCall='Highest Premium/Delta'

    Raises ValueError on parse failure so the caller can abort before commit.
    """
    call_name = f"{plan_name} CALL"
    put_name = f"{plan_name} PUT"
    logging.info("Updating template values for %s / %s...", call_name, put_name)

    try:
        spread     = float(str(first_row["Spread"]).replace('"', "").strip())
        stop_loss  = float(str(first_row["Stop"]).lower().replace("x", "").strip())
        target_max = float(first_row["Premium"])
    except (ValueError, KeyError) as exc:
        raise ValueError(
            f"Cannot parse required CSV fields (Spread/Stop/Premium): {exc}"
        ) from exc

    # CSV Spread → string form for LongWidth/LongWidthCall ("100" not "100.0").
    width_str = f"{spread:g}"

    logging.info(
        "CSV values — Premium(TargetMax): %s, Spread(LongMaxWidth/LongWidth): %s, Stop: %s",
        target_max, spread, stop_loss,
    )

    shared: dict = {
        "StopMultiple":     stop_loss,
        "LongWidth":        "",
        "LongWidthCall":    width_str,
        "LongMaxWidthCall": spread,
        "Preference":       _PREF_HIGHEST,
        "PreferenceCall":   _PREF_HIGHEST,
    }
    call_sets: dict = {
        **shared,
        "TargetMaxCall":      target_max,
        "TargetMax":          0.0,
        "LongType":           _LONG_TYPE_WIDTH,
        "LongTypeCall":       _LONG_TYPE_MAX_PREMIUM,
        "LongMaxPremium":     None,
        "LongMaxPremiumCall": _LONG_MAX_PREMIUM_ACTIVE,
        "LongMaxWidth":       None,
    }
    put_sets: dict = {
        **shared,
        "TargetMax":          target_max,
        "TargetMaxCall":      target_max,
        "LongType":           _LONG_TYPE_MAX_PREMIUM,
        "LongTypeCall":       _LONG_TYPE_MAX_PREMIUM,
        "LongMaxPremium":     _LONG_MAX_PREMIUM_ACTIVE,
        "LongMaxPremiumCall": _LONG_MAX_PREMIUM_ACTIVE,
        "LongMaxWidth":       spread,
    }

    for tmpl_name, sets in ((call_name, call_sets), (put_name, put_sets)):
        # Defense-in-depth: the allowlist filter is also enforced at INSERT
        # time in ensure_template_exists; honoring it here keeps the security
        # boundary intact for UPDATE paths too.
        safe = {k: v for k, v in sets.items() if k in _ALLOWED_TEMPLATE_COLS}
        set_clause = ", ".join(f"{col} = ?" for col in safe)
        cursor.execute(
            f"UPDATE TradeTemplate SET {set_clause} WHERE Name = ?",
            [*safe.values(), tmpl_name],
        )

    logging.info(
        "  -> Templates updated: TargetMax=%s, LongMaxWidth=%s, LongWidth='%s', Stop=%s",
        target_max, spread, width_str, stop_loss,
    )


# ── Skip-day handling ─────────────────────────────────────────────────────────
def deactivate_plan(cursor: sqlite3.Cursor, plan_name: str) -> int:
    """Delete every ScheduleMaster row matching *plan_name*.

    Used when the upstream tradeplan CSV is empty — i.e. the engine's regime
    filter (multi_day_return, prior_day_return, etc.) blocked the target
    market day. Leaving yesterday's schedules in place would cause TAT to
    fire stale entries on a day the strategy explicitly opted out of.

    Returns the row count deleted. The caller is responsible for committing.
    """
    pattern = f"%{plan_name}%"
    cursor.execute(
        "DELETE FROM ScheduleMaster WHERE Strategy LIKE ? OR DisplayStrategy LIKE ?",
        (pattern, pattern),
    )
    deleted = cursor.rowcount
    logging.info(
        "  -> SKIP DAY: deleted %d existing schedules for plan '%s'.",
        deleted, plan_name,
    )
    return int(deleted)


def derive_plan_name_from_csv_path(csv_path: Path, override: str | None) -> str:
    """Pick the plan name when the CSV body can't supply it (empty CSV).

    Priority:
      1. Explicit `--plan` CLI override
      2. Filename pattern `tradeplan-<NAME>.csv` produced by `strategy_v17.sh`
      3. Hard fail — refuse to guess and risk deleting an unrelated plan
    """
    if override:
        return override.strip()
    stem = csv_path.stem  # tradeplan-P2
    if stem.startswith("tradeplan-") and len(stem) > len("tradeplan-"):
        return stem[len("tradeplan-"):]
    raise ValueError(
        f"Cannot derive plan name from empty CSV at {csv_path}. "
        "Pass --plan <NAME> explicitly, or name the file tradeplan-<NAME>.csv."
    )


# ── Schedule management ────────────────────────────────────────────────────────
def create_schedules(
    cursor: sqlite3.Cursor,
    df: "pd.DataFrame",
    plan: SchedulePlan,
) -> None:
    """Deletes and recreates the SPLIT schedules (CALL + PUT) for *plan* from *df*."""
    logging.info(
        "Recreating SPLIT Schedules for Plan '%s' (Account: %s)...",
        plan.plan_name, plan.account_number,
    )

    pattern = f"%{plan.plan_name}%"
    cursor.execute(
        "DELETE FROM ScheduleMaster WHERE Strategy LIKE ? OR DisplayStrategy LIKE ?",
        (pattern, pattern),
    )
    logging.info("  -> Deleted %d old schedules matching '%s'.", cursor.rowcount, plan.plan_name)

    base: dict = {
        "ScheduleType": "Trade",
        "Account": plan.account_number,
        "QtyType": "FixedQty",
        "QtyOverride": 1,
        "ExpirationMinutes": 1,
        "IsActive": 1,
        "DayMonday": 1, "DayTuesday": 1, "DayWednesday": 1,
        "DayThursday": 1, "DayFriday": 1,
        "TradeTemplateID": 0,
        "TradeTemplateIDFailure": 0,
        "TradeConditionID": 0,
        "DisplayTemplate": "",
        "DisplayStrategy": "",
        "DisplayCondition": "",
    }
    live_cols = set(get_table_columns(cursor, "ScheduleMaster")) - {"ScheduleMasterID"}
    count_call = count_put = 0

    for idx, row in df.iterrows():
        try:
            hour, minute = map(int, str(row["Hour:Minute"]).split(":"))
        except ValueError:
            logging.warning(
                "Row %s: invalid Hour:Minute value %r — skipped.",
                idx, row.get("Hour:Minute"),
            )
            continue

        qty: int = 1
        if plan.qty_override is not None:
            qty = plan.qty_override
        else:
            qty_col = "Qty" if "Qty" in row else ("qty" if "qty" in row else None)
            if qty_col and pd.notna(row[qty_col]):
                try:
                    qty = int(row[qty_col])
                except ValueError:
                    qty = 1

        for side, tmpl_id, cond_id, cond_name in (
            ("CALL", plan.call_template_id, plan.call_condition_id, plan.call_condition_name),
            ("PUT",  plan.put_template_id,  plan.put_condition_id,  plan.put_condition_name),
        ):
            sched = {
                **base,
                "Hour": hour,
                "Minute": minute,
                "QtyOverride": qty,
                "TradeTemplateID": tmpl_id,
                "TradeConditionID": cond_id,
                "DisplayTemplate": f"{plan.plan_name} {side}",
                "DisplayStrategy": f"{plan.plan_name} {side}",
                "DisplayCondition": cond_name,
            }
            clean = {
                k: v for k, v in sched.items()
                if k in live_cols and k in _ALLOWED_SCHEDULE_COLS
            }
            cols = ", ".join(clean)
            placeholders = ", ".join("?" * len(clean))
            cursor.execute(
                f"INSERT INTO ScheduleMaster ({cols}) VALUES ({placeholders})",
                list(clean.values()),
            )
            if side == "CALL":
                count_call += 1
            else:
                count_put += 1

    logging.info(
        "  -> Created %d CALL + %d PUT schedules (total %d).",
        count_call, count_put, count_call + count_put,
    )


# ── Entry point ────────────────────────────────────────────────────────────────
def main() -> None:
    setup_logging()
    args = parse_arguments()
    cfg = load_config()

    account_number = cfg.get("account", "number").strip()
    db_path = Path(cfg.get("paths", "db", fallback=str(_DIR / "data.db3")))
    csv_path = Path(args.csv) if args.csv else Path(
        cfg.get("paths", "csv", fallback=str(_DIR / "tradeplan.csv"))
    )

    logging.info("Starting Tradeplan Update Process...")
    logging.info("tradeplan2db3.py v%s — %s (last updated %s)",
                 __version__, __build__, __updated__)
    logging.info("  DB : %s", db_path)
    logging.info("  CSV: %s", csv_path)

    if not db_path.exists():
        sys.exit(f"ERROR: Database not found: {db_path}")
    if not csv_path.exists():
        sys.exit(f"ERROR: Tradeplan CSV not found: {csv_path}")

    # Backup — abort hard if it fails
    try:
        backup_path = backup_database(db_path)
        logging.info("Backup OK: %s", backup_path)
    except RuntimeError as exc:
        sys.exit(f"ERROR: {exc}\nAborting to protect the live database.")

    # Read CSV. Two flavours of "skip day" can land here:
    #   1. SKIP-marker CSV (preferred): header + 1 row with "SKIP" in the
    #      Hour:Minute cell and the plan name in the Plan column. Survives
    #      any filename change on the TAT side (which often renames the
    #      uploaded file to the INI's generic `tradeplan.csv` default).
    #   2. Truly empty CSV (legacy): 0 bytes, no header. Plan name has to be
    #      derived from `--plan` or the `tradeplan-<NAME>.csv` filename.
    try:
        df = pd.read_csv(csv_path)
        df.columns = df.columns.str.strip().str.replace('"', "", regex=False)
    except pd.errors.EmptyDataError:
        df = pd.DataFrame()
    except (OSError, pd.errors.ParserError) as exc:
        sys.exit(f"ERROR reading CSV: {exc}")

    # Flavour 1 — SKIP marker row inside a non-empty CSV.
    if not df.empty and len(df) == 1 and "Hour:Minute" in df.columns:
        first_cell = str(df.iloc[0]["Hour:Minute"]).strip().upper()
        if first_cell == "SKIP":
            plan_name = str(df.iloc[0].get("Plan", "")).strip() or args.plan
            if not plan_name:
                sys.exit(
                    "ERROR: SKIP marker row missing Plan column — cannot "
                    "determine which schedules to deactivate. Pass --plan."
                )
            logging.info(
                "SKIP marker row in %s — treating as SKIP DAY for plan '%s'.",
                csv_path, plan_name,
            )
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                try:
                    deleted = deactivate_plan(cursor, plan_name)
                    conn.commit()
                    logging.info(
                        "SKIP DAY committed: %d schedules deleted for plan '%s'. "
                        "TAT will fire nothing for this plan today.",
                        deleted, plan_name,
                    )
                except sqlite3.Error as exc:
                    conn.rollback()
                    logging.error("Database error during skip-day deactivation: %s", exc)
                    sys.exit(f"Database error: {exc}")
            return

    # Flavour 2 — truly empty CSV (legacy fallback).
    if df.empty:
        try:
            plan_name = derive_plan_name_from_csv_path(csv_path, args.plan)
        except ValueError as exc:
            sys.exit(f"ERROR: {exc}")
        logging.info(
            "Empty tradeplan CSV at %s — treating as SKIP DAY for plan '%s'.",
            csv_path, plan_name,
        )
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            try:
                deleted = deactivate_plan(cursor, plan_name)
                conn.commit()
                logging.info(
                    "SKIP DAY committed: %d schedules deleted for plan '%s'. "
                    "TAT will fire nothing for this plan today.",
                    deleted, plan_name,
                )
            except sqlite3.Error as exc:
                conn.rollback()
                logging.error("Database error during skip-day deactivation: %s", exc)
                sys.exit(f"Database error: {exc}")
        return

    warn_inconsistent_rows(df)

    first_row = df.iloc[0]
    plan_name = str(first_row.get("Plan", "P1")).strip()
    strategy_col = str(first_row.get("Strategy", "")).lower().strip()

    if strategy_col not in STRATEGY_MAP:
        sys.exit(
            f"ERROR: Unknown strategy '{strategy_col}'. "
            f"Valid values: {sorted(STRATEGY_MAP)}"
        )
    input_val, comp_val = STRATEGY_MAP[strategy_col]

    cond_call_name = f"{input_val} < {comp_val}"
    cond_put_name  = f"{input_val} > {comp_val}"

    logging.info("Plan: '%s'  Strategy: '%s'", plan_name, strategy_col)
    logging.info("  Call condition: '%s'", cond_call_name)
    logging.info("  Put  condition: '%s'", cond_put_name)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        try:
            call_tmpl_id = ensure_template_exists(cursor, TEMPLATE_BASE_CALL, f"{plan_name} CALL")
            put_tmpl_id  = ensure_template_exists(cursor, TEMPLATE_BASE_PUT,  f"{plan_name} PUT")
            call_cond_id = ensure_condition_exists(cursor, cond_call_name, input_val, comp_val, "<")
            put_cond_id  = ensure_condition_exists(cursor, cond_put_name,  input_val, comp_val, ">")

            update_template_values(cursor, first_row, plan_name)

            sched_plan = SchedulePlan(
                plan_name=plan_name,
                call_template_id=call_tmpl_id,
                put_template_id=put_tmpl_id,
                call_condition_id=call_cond_id,
                put_condition_id=put_cond_id,
                call_condition_name=cond_call_name,
                put_condition_name=cond_put_name,
                account_number=account_number,
                qty_override=args.qty,
            )
            create_schedules(cursor, df, sched_plan)

            conn.commit()
            logging.info("Database update committed successfully.")

        except (ValueError, RuntimeError) as exc:
            conn.rollback()
            logging.error("Aborting: %s", exc)
            sys.exit(f"ERROR: {exc}")
        except sqlite3.Error as exc:
            conn.rollback()
            logging.error("Database error: %s", exc)
            sys.exit(f"Database error: {exc}")

    logging.info("Done.")


if __name__ == "__main__":
    main()
