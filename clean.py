#!/usr/bin/env python3
import duckdb
import os
import logging
from typing import Optional, Dict, Tuple

# ---------------- Paths & Logging ----------------
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, "nyc_taxi.duckdb")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=os.path.join(LOGS_DIR, "clean.log"),
)
logger = logging.getLogger(__name__)

# ------------- SQL helpers -------------
# Column set used to identify duplicate trips (no trip id in minimal schema)
DUP_KEYS = """
    pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
    pu_location_id, do_location_id, total_amount, cab_color
"""

def make_clean_sql(raw_table: str, clean_table: str) -> str:
    """
    Create SQL that:
      1) de-duplicates by ROW_NUMBER() over the full row signature
      2) applies cleaning filters per rubric
         - passenger_count <> 0
         - trip_distance > 0
         - trip_distance <= 100
         - duration <= 86400 seconds
    """
    return f"""
    CREATE OR REPLACE TABLE {clean_table} AS
    WITH de_duped AS (
        SELECT
            {DUP_KEYS},
            ROW_NUMBER() OVER (
                PARTITION BY {DUP_KEYS}
                ORDER BY pickup_datetime
            ) AS rn
        FROM {raw_table}
    )
    SELECT
        pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
        pu_location_id, do_location_id, total_amount, cab_color
    FROM de_duped
    WHERE rn = 1
      AND passenger_count <> 0
      AND trip_distance > 0
      AND trip_distance <= 100
      AND date_diff('second', pickup_datetime, dropoff_datetime) <= 86400;
    """

def make_tests(table: str) -> Dict[str, str]:
    """
    Build validation queries (each must return 0 rows to PASS):
      - duplicates remaining
      - trips with 0 passengers
      - trips with 0 miles
      - trips > 100 miles
      - trips > 24 hours (86400 sec)
    """
    return {
        # Count groups with more than 1 identical record
        "duplicates_remaining": f"""
            SELECT COUNT(*) FROM (
                SELECT 1
                FROM {table}
                GROUP BY {DUP_KEYS}
                HAVING COUNT(*) > 1
            );
        """,
        "zero_passengers": f"""
            SELECT COUNT(*) FROM {table}
            WHERE passenger_count = 0;
        """,
        "zero_miles": f"""
            SELECT COUNT(*) FROM {table}
            WHERE trip_distance = 0;
        """,
        "over_100_miles": f"""
            SELECT COUNT(*) FROM {table}
            WHERE trip_distance > 100;
        """,
        "over_24_hours": f"""
            SELECT COUNT(*) FROM {table}
            WHERE date_diff('second', pickup_datetime, dropoff_datetime) > 86400;
        """,
    }

def run_tests(con: duckdb.DuckDBPyConnection, table: str) -> Tuple[bool, Dict[str, int]]:
    """
    Execute validation queries and return:
      - overall pass/fail
      - dict of test_name -> count
    """
    results: Dict[str, int] = {}
    tests = make_tests(table)
    all_ok = True
    for name, sql in tests.items():
        cnt = con.execute(sql).fetchone()[0]
        results[name] = cnt
        if cnt != 0:
            all_ok = False
    return all_ok, results

def pretty_report(label: str, ok: bool, counts: Dict[str, int]):
    """
    Print & log a clean PASS/FAIL report for a table’s tests.
    """
    header = f"[{label}] CLEANING TESTS {'PASS' if ok else 'FAIL'}"
    line = "-" * len(header)
    print(line)
    print(header)
    print(line)
    for k, v in counts.items():
        print(f"{k:>20}: {v}")
    print()
    logger.info("%s", header)
    for k, v in counts.items():
        logger.info("%s: %s", k, v)

def clean_table(con: duckdb.DuckDBPyConnection, raw_table: str, clean_table: str, label: str):
    """
    Build cleaned table from raw:
      - de-dup
      - apply filters per rubric
      - report pre/post rows
      - run & report tests
    """
    # Row counts for context
    raw_cnt = con.execute(f"SELECT COUNT(*) FROM {raw_table}").fetchone()[0]
    print(f"{label} raw row count: {raw_cnt}")
    logger.info("%s raw row count: %s", label, raw_cnt)

    # Build the cleaned table
    con.execute(make_clean_sql(raw_table, clean_table))

    clean_cnt = con.execute(f"SELECT COUNT(*) FROM {clean_table}").fetchone()[0]
    print(f"{label} CLEAN row count: {clean_cnt}")
    logger.info("%s CLEAN row count: %s", label, clean_cnt)

    # Verification tests
    ok, counts = run_tests(con, clean_table)
    pretty_report(f"{label}: {clean_table}", ok, counts)

def main():
    """
    - Connect to DuckDB
    - Clean yellow_raw -> yellow_clean
    - Clean green_raw  -> green_clean
    - Run rubric-mandated tests, print & log outputs
    """
    con: Optional[duckdb.DuckDBPyConnection] = None
    try:
        con = duckdb.connect(database=DB_PATH, read_only=False)

        # Clean Yellow
        clean_table(con, "yellow_raw", "yellow_clean", "YELLOW")

        # Clean Green
        clean_table(con, "green_raw", "green_clean", "GREEN")

        print("\nCleaning complete.")
        print(f"Log file: {os.path.join(LOGS_DIR, 'clean.log')}")
    except Exception as e:
        logger.exception("Cleaning failed: %s", e)
        print(f"An error occurred: {e}")
    finally:
        if con is not None:
            con.close()

if __name__ == "__main__":
    main()
