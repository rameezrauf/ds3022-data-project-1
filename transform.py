#!/usr/bin/env python3
"""
transform.py
------------
Transforms yellow_clean and green_clean into yellow_trips and green_trips with:
- trip_co2_kgs (kg CO2 per trip)
- avg_mph (average speed)
- hour_of_day, day_of_week, week_of_year, month_of_year (time features)

To avoid OOM/segfault, transformations are chunked month-by-month.
"""

import duckdb
import os
import logging
from typing import Optional

# ---------------- Paths & Logging ----------------
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
TMP_DIR  = os.path.join(DATA_DIR, "tmp")  # for DuckDB spill files
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(TMP_DIR,  exist_ok=True)

DB_PATH  = os.path.join(DATA_DIR, "nyc_taxi.duckdb")
LOG_PATH = os.path.join(LOGS_DIR, "transform.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=LOG_PATH,
)
logger = logging.getLogger(__name__)

# ---------------- Helpers ----------------
def ensure_target_schema(con: duckdb.DuckDBPyConnection, clean: str, target: str):
    """Create empty target table with final schema."""
    con.execute(f"""
        CREATE OR REPLACE TABLE {target} AS
        SELECT
            pickup_datetime,
            dropoff_datetime,
            passenger_count,
            trip_distance,
            pu_location_id,
            do_location_id,
            total_amount,
            CAST(NULL AS VARCHAR) AS cab_color,
            CAST(NULL AS DOUBLE)  AS trip_co2_kgs,
            CAST(NULL AS DOUBLE)  AS avg_mph,
            CAST(NULL AS INT)     AS hour_of_day,
            CAST(NULL AS INT)     AS day_of_week,
            CAST(NULL AS INT)     AS week_of_year,
            CAST(NULL AS INT)     AS month_of_year
        FROM {clean}
        WHERE 1=0
    """)

def transform_month(con: duckdb.DuckDBPyConnection, clean: str, target: str, color: str, year: int, month: int) -> int:
    """Transform a single month’s rows into target table."""
    n_src = con.execute(f"""
        SELECT COUNT(*) FROM {clean}
        WHERE date_part('year', pickup_datetime)={year}
          AND date_part('month', pickup_datetime)={month}
    """).fetchone()[0]
    if n_src == 0:
        return 0

    sql = f"""
        INSERT INTO {target}
        WITH base AS (
            SELECT
                pickup_datetime,
                dropoff_datetime,
                passenger_count,
                trip_distance,
                pu_location_id,
                do_location_id,
                total_amount,
                EPOCH(dropoff_datetime) - EPOCH(pickup_datetime) AS duration_sec
            FROM {clean}
            WHERE date_part('year', pickup_datetime)={year}
              AND date_part('month', pickup_datetime)={month}
        ),
        joined AS (
            SELECT
                b.*,
                ve.co2_grams_per_mile
            FROM base b
            LEFT JOIN vehicle_emissions ve
              ON LOWER(ve.vehicle_type)=LOWER('{color}_taxi')
        )
        SELECT
            pickup_datetime,
            dropoff_datetime,
            passenger_count,
            trip_distance,
            pu_location_id,
            do_location_id,
            total_amount,
            '{color}' AS cab_color,
            ROUND((trip_distance * COALESCE(co2_grams_per_mile, 300)) / 1000.0, 6) AS trip_co2_kgs,
            CASE WHEN duration_sec > 0
                 THEN trip_distance / (duration_sec / 3600.0)
                 ELSE NULL END AS avg_mph,
            date_part('hour', pickup_datetime)  AS hour_of_day,
            dayofweek(pickup_datetime)          AS day_of_week,
            date_part('week', pickup_datetime)  AS week_of_year,
            month(pickup_datetime)              AS month_of_year
        FROM joined
    """
    con.execute(sql)

    print(f"{color}: {year}-{month:02d} inserted {n_src:,} rows")
    logger.info("%s: %s-%02d inserted %s rows", color, year, month, n_src)
    return n_src

def transform_color(con: duckdb.DuckDBPyConnection, clean: str, target: str, color: str):
    """Transform a full color (yellow/green) month by month."""
    total_src = con.execute(f"SELECT COUNT(*) FROM {clean}").fetchone()[0]
    print(f"{color}: clean rows: {total_src:,}")
    logger.info("%s: clean rows: %s", color, total_src)

    ensure_target_schema(con, clean, target)

    inserted_total = 0
    for y in range(2015, 2025):
        for m in range(1, 13):
            inserted_total += transform_month(con, clean, target, color, y, m)

    out_rows = con.execute(f"SELECT COUNT(*) FROM {target}").fetchone()[0]
    print(f"{color}: transformed rows: {out_rows:,} (inserted this run: {inserted_total:,})")
    logger.info("%s: transformed rows: %s (inserted this run: %s)", color, out_rows, inserted_total)

# ---------------- Main ----------------
def main():
    con: Optional[duckdb.DuckDBPyConnection] = None
    try:
        con = duckdb.connect(DB_PATH, read_only=False)
        print(f"Connected: {DB_PATH}")
        logger.info("Connected DB: %s", DB_PATH)

        # force spills to disk if memory is tight
        con.execute(f"PRAGMA temp_directory='{TMP_DIR}'")

        transform_color(con, "yellow_clean", "yellow_trips", "yellow")
        transform_color(con, "green_clean",  "green_trips",  "green")

        print("\nTransformation complete.")
        print(f"Log file: {LOG_PATH}")
    except Exception as e:
        logger.exception("Transformation failed: %s", e)
        print(f"An error occurred: {e}")
    finally:
        if con:
            con.close()

if __name__ == "__main__":
    main()