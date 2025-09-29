#!/usr/bin/env python3
import duckdb
import os
import logging
from typing import Optional, Tuple, Iterable

# ---------------- Paths & Logging ----------------
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
TMP_DIR  = os.path.join(DATA_DIR, "tmp")  # for spill files
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(TMP_DIR,  exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, "nyc_taxi.duckdb")
LOG_PATH = os.path.join(LOGS_DIR, "transform.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=LOG_PATH,
)
logger = logging.getLogger(__name__)

# Project scope years
REQUIRED_YEARS = list(range(2015, 2025))


# ---------------- Helpers ----------------
def emission_factors(con: duckdb.DuckDBPyConnection) -> Tuple[float, float, float]:
    """
    Return (yellow_gpm, green_gpm, default_gpm) from vehicle_emissions.
    """
    y, g, d = con.execute("""
        SELECT
            AVG(CASE WHEN LOWER(vehicle_type) LIKE '%yellow%' THEN co2_grams_per_mile END),
            AVG(CASE WHEN LOWER(vehicle_type) LIKE '%green%'  THEN co2_grams_per_mile END),
            AVG(co2_grams_per_mile)
        FROM vehicle_emissions
    """).fetchone()
    return (y or 0.0, g or 0.0, d or 0.0)


def ensure_target_schema(con: duckdb.DuckDBPyConnection, clean_table: str, target_table: str) -> None:
    """
    Create empty target_table with the final schema (CREATE … AS SELECT … WHERE FALSE).
    """
    con.execute(f"""
        CREATE OR REPLACE TABLE {target_table} AS
        SELECT
            pickup_datetime,
            dropoff_datetime,
            passenger_count,
            trip_distance,
            pu_location_id,
            do_location_id,
            total_amount,
            LOWER(TRIM(cab_color)) AS cab_color,
            CAST(NULL AS DOUBLE) AS trip_co2_kgs,
            CAST(NULL AS DOUBLE) AS avg_mph,
            CAST(NULL AS INT)    AS hour_of_day,
            CAST(NULL AS INT)    AS day_of_week,
            CAST(NULL AS INT)    AS week_of_year,
            CAST(NULL AS INT)    AS month_of_year
        FROM {clean_table}
        WHERE 1=0
    """)


def years_present(con: duckdb.DuckDBPyConnection, clean_table: str) -> Iterable[int]:
    """
    Return distinct years present in the clean table within the required scope.
    """
    rows = con.execute(f"""
        SELECT DISTINCT date_part('year', pickup_datetime)::INT AS y
        FROM {clean_table}
        WHERE pickup_datetime IS NOT NULL
          AND pickup_datetime >= TIMESTAMP '2015-01-01'
          AND pickup_datetime <  TIMESTAMP '2025-01-01'
        ORDER BY y
    """).fetchall()
    return [int(r[0]) for r in rows]


def insert_year_chunk(
    con: duckdb.DuckDBPyConnection,
    clean_table: str,
    target_table: str,
    y_gpm: float,
    g_gpm: float,
    d_gpm: float,
    year: int,
) -> int:
    """
    Insert a single year's rows with all derived columns into target_table.
    Returns number of rows inserted for this year.
    """
    # rows available for this year?
    n_src = con.execute(f"""
        SELECT COUNT(*) FROM {clean_table}
        WHERE pickup_datetime IS NOT NULL
          AND date_part('year', pickup_datetime) = {year}
    """).fetchone()[0]
    if n_src == 0:
        print(f"  Year {year}: (no rows) — skipping")
        logger.info("Year %s: no rows, skipped", year)
        return 0

    # Choose fallback factor based on color per row
    fallback_expr = f"""
        CASE
            WHEN LOWER(TRIM(cab_color)) = 'yellow' THEN {y_gpm}
            WHEN LOWER(TRIM(cab_color)) = 'green'  THEN {g_gpm}
            ELSE {d_gpm}
        END
    """

    sql = f"""
    INSERT INTO {target_table}
    WITH base AS (
        SELECT
            pickup_datetime,
            dropoff_datetime,
            passenger_count,
            trip_distance,
            pu_location_id,
            do_location_id,
            total_amount,
            LOWER(TRIM(cab_color)) AS cab_color,
            CAST(date_diff('second', pickup_datetime, dropoff_datetime) AS DOUBLE) AS duration_sec
        FROM {clean_table}
        WHERE pickup_datetime IS NOT NULL
          AND date_part('year', pickup_datetime) = {year}
          AND pickup_datetime >= TIMESTAMP '2015-01-01'
          AND pickup_datetime <  TIMESTAMP '2025-01-01'
    ),
    joined AS (
        SELECT
            b.*,
            ve.co2_grams_per_mile
        FROM base b
        LEFT JOIN vehicle_emissions ve
          ON LOWER(ve.vehicle_type) IN (b.cab_color, b.cab_color || '_taxi')
    )
    SELECT
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        pu_location_id,
        do_location_id,
        total_amount,
        cab_color,
        /* CO2 in kg with robust fallback */
        ROUND(
            (trip_distance * COALESCE(co2_grams_per_mile, {fallback_expr})) / 1000.0, 6
        ) AS trip_co2_kgs,
        /* avg mph (guard against zero or NULL) */
        CASE
            WHEN duration_sec IS NOT NULL AND duration_sec > 0
                THEN trip_distance / (duration_sec / 3600.0)
            ELSE NULL
        END AS avg_mph,
        date_part('hour', pickup_datetime)  AS hour_of_day,
        dayofweek(pickup_datetime)          AS day_of_week,
        date_part('week', pickup_datetime)  AS week_of_year,
        month(pickup_datetime)              AS month_of_year
    FROM joined
    """
    con.execute(sql)

    # quick stats for this year; coalesce Nones so printing never crashes
    nulls = con.execute(f"""
        SELECT
          SUM(trip_co2_kgs IS NULL),
          SUM(avg_mph IS NULL),
          COUNT(*)
        FROM {target_table}
        WHERE date_part('year', pickup_datetime) = {year}
    """).fetchone()
    null_co2    = int(nulls[0] or 0)
    null_avgmph = int(nulls[1] or 0)
    n_out       = int(nulls[2] or 0)

    print(f"  Year {year}: inserted {n_out:,} rows — NULLs (co2={null_co2:,}, avg_mph={null_avgmph:,})")
    logger.info("Year %s: inserted %s rows; nulls co2=%s, avg_mph=%s", year, n_out, null_co2, null_avgmph)
    return n_out


def transform_color(con: duckdb.DuckDBPyConnection, clean_table: str, target_table: str, color_label: str) -> None:
    """
    Transform a single color in year-sized chunks to avoid OOM and segfaults.
    """
    total_src = con.execute(f"SELECT COUNT(*) FROM {clean_table}").fetchone()[0]
    print(f"{color_label}: clean rows: {total_src:,}")
    logger.info("%s: clean rows: %s", color_label, total_src)

    ensure_target_schema(con, clean_table, target_table)

    y_gpm, g_gpm, d_gpm = emission_factors(con)
    logger.info("Emission factors: yellow=%.3f g/mi, green=%.3f g/mi, default=%.3f g/mi", y_gpm, g_gpm, d_gpm)

    present = set(years_present(con, clean_table))
    print(f"{color_label}: processing years 2015–2024 in chunks ...")
    logger.info("%s: processing years 2015–2024", color_label)

    total_inserted = 0
    for year in REQUIRED_YEARS:
        if year not in present:
            print(f"  Year {year}: (no rows) — skipping")
            logger.info("Year %s: (no rows) — skipping", year)
            continue
        total_inserted += insert_year_chunk(con, clean_table, target_table, y_gpm, g_gpm, d_gpm, year)

    out_rows = con.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]
    print(f"{color_label}: transformed rows: {out_rows:,} (inserted this run: {total_inserted:,})")
    logger.info("%s: transformed rows: %s (inserted this run: %s)", color_label, out_rows, total_inserted)

    if out_rows != total_src:
        msg = f"{color_label}: Row count mismatch (clean={total_src:,}, transformed={out_rows:,})."
        print("NOTICE:", msg)
        logger.warning(msg)


def main():
    con: Optional[duckdb.DuckDBPyConnection] = None
    try:
        con = duckdb.connect(database=DB_PATH, read_only=False)
        print(f"Connected: {DB_PATH}")
        logger.info("Connected DB: %s", DB_PATH)

        # Use a local spill directory to reduce memory pressure
        con.execute(f"PRAGMA temp_directory='{TMP_DIR}';")

        # sanity check: emissions exists
        con.execute("SELECT 1 FROM vehicle_emissions LIMIT 1")

        transform_color(con, "yellow_clean", "yellow_trips", "YELLOW")
        transform_color(con, "green_clean",  "green_trips",  "GREEN")

        print("\nTransformation complete.")
        print(f"Log file: {LOG_PATH}")
    except Exception as e:
        logger.exception("Transformation failed: %s", e)
        print(f"An error occurred: {e}")
    finally:
        if con is not None:
            con.close()


if __name__ == "__main__":
    main()