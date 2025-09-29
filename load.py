#!/usr/bin/env python3
"""
load.py
-------
Loads NYC Taxi Trip data for Yellow & Green cabs across 2015–2024 into a local DuckDB.
- Downloads Parquet files to ./data/raw/<type>/<year>/
- Loads vehicle_emissions.csv from ./data/
- Normalizes schema differences across years and inserts into yellow_raw, green_raw
- Prints/logs raw row counts + richer descriptive stats (pre-cleaning), per rubric

Log file: load.log
Database: ./data/nyc_taxi.duckdb
"""

import logging
import time
from pathlib import Path
from typing import List, Tuple

import requests
import duckdb

# ------------------ logging ------------------
LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

#logger code
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "load.log"),
        logging.StreamHandler()
    ],
)
log = logging.getLogger("load")

# ------------------ constants ------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
DB_PATH = DATA_DIR / "nyc_taxi.duckdb"
VEHICLE_EMISSIONS_CSV = DATA_DIR / "vehicle_emissions.csv"

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
YEARS = list(range(2015, 2025))  # 2015–2024 inclusive (10 years for bonus)
MONTHS = [f"{m:02d}" for m in range(1, 13)]

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)
TIMEOUT = 60
RETRIES = 3
BACKOFF = 2.0


# ------------------ helpers ------------------
def ensure_dirs() -> None:
    """Ensure base data and raw folders exist for both taxi types across all YEARS."""
    DATA_DIR.mkdir(exist_ok=True)
    for t in ("yellow", "green"):
        for y in YEARS:
            (RAW_DIR / t / str(y)).mkdir(parents=True, exist_ok=True)


def url_and_dest(taxi_type: str, year: int, month: str) -> Tuple[str, Path]:
    """Build cloud URL and local destination path for a given taxi type/year/month."""
    fname = f"{taxi_type}_tripdata_{year}-{month}.parquet"
    url = f"{BASE_URL}/{fname}"
    dest = RAW_DIR / taxi_type / str(year) / fname
    return url, dest


def download_with_retries(url: str, dest: Path) -> bool:
    """
    Download a file with simple retry/backoff and a desktop-like user agent.
    Returns True if the file exists (downloaded or already present).
    """
    if dest.exists() and dest.stat().st_size > 0:
        return True
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(1, RETRIES + 1):
        try:
            with requests.get(url, headers=headers, stream=True, timeout=TIMEOUT) as r:
                if r.status_code == 200:
                    tmp = dest.with_suffix(".part")
                    with open(tmp, "wb") as f:
                        for chunk in r.iter_content(1024 * 1024):
                            if chunk:
                                f.write(chunk)
                    tmp.rename(dest)
                    log.info(f"Downloaded {dest.name}")
                    return True
                else:
                    # 403s/404s are common for missing months/types—just warn and continue
                    log.warning(f"[{r.status_code}] {url}")
        except Exception as e:
            log.warning(f"Attempt {attempt}/{RETRIES} failed for {url}: {e}")
        time.sleep(BACKOFF * attempt)
    return dest.exists() and dest.stat().st_size > 0


def local_files(taxi_type: str) -> List[str]:
    """Return sorted list of all local parquet files for a taxi type across all YEARS."""
    files: List[str] = []
    for y in YEARS:
        folder = RAW_DIR / taxi_type / str(y)
        files.extend(str(p) for p in sorted(folder.glob("*.parquet")))
    return files


# ------------------ load steps ------------------
def load_vehicle_emissions(con: duckdb.DuckDBPyConnection) -> None:
    """load vehicle_emissions lookup from CSV under ./data/."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS vehicle_emissions (
            vehicle_type VARCHAR,
            co2_grams_per_mile DOUBLE
        );
        DELETE FROM vehicle_emissions;
    """)
    con.execute(
        "INSERT INTO vehicle_emissions SELECT * FROM read_csv_auto(?, header=True)",
        [str(VEHICLE_EMISSIONS_CSV)],
    )
    n = con.execute("SELECT COUNT(*) FROM vehicle_emissions").fetchone()[0]
    print(f"vehicle_emissions raw row count: {n:,}")
    log.info(f"vehicle_emissions rows: {n:,}")


def create_raw_tables(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create raw tables (idempotent) and clear them for a fresh load.
    We keep a minimal, stable schema that works across all years.
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS yellow_raw (
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            pu_location_id INTEGER,
            do_location_id INTEGER,
            payment_type INTEGER,
            total_amount DOUBLE
        );
        CREATE TABLE IF NOT EXISTS green_raw (
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            pu_location_id INTEGER,
            do_location_id INTEGER,
            payment_type INTEGER,
            total_amount DOUBLE
        );
        DELETE FROM yellow_raw;
        DELETE FROM green_raw;
    """)


def insert_trips(con: duckdb.DuckDBPyConnection, taxi_type: str):
    """
    Insert normalized rows into {taxi_type}_raw for the selected years.

    - For yellow -> use tpep_* timestamp columns
    - For green  -> use lpep_* timestamp columns
    - Introspects the Parquet schema and only references columns that exist.
      If a column like PULocationID/DOLocationID is missing in older months,
      we insert NULL for that column to keep a consistent target schema.
    """
    files = local_files(taxi_type)
    if not files:
        log.warning(f"No local files for {taxi_type}. Skipping insert.")
        return

    # Peek at the schema across all files (no rows) to know which columns exist
    # union_by_name=true aligns differing monthly schemas
    tmp = con.execute(
        "SELECT * FROM read_parquet(?, union_by_name=true) LIMIT 0",
        [files],
    )
    cols = {c[0].lower() for c in tmp.description}  # column names present

    # Timestamp columns by type
    if taxi_type == "yellow":
        pickup_expr  = "tpep_pickup_datetime"
        dropoff_expr = "tpep_dropoff_datetime"
        if "tpep_pickup_datetime" not in cols or "tpep_dropoff_datetime" not in cols:
            raise RuntimeError("Expected tpep_* columns not found in yellow files.")
    else:
        pickup_expr  = "lpep_pickup_datetime"
        dropoff_expr = "lpep_dropoff_datetime"
        if "lpep_pickup_datetime" not in cols or "lpep_dropoff_datetime" not in cols:
            raise RuntimeError("Expected lpep_* columns not found in green files.")

    # Optional columns (exist in most modern months; may be absent in older files)
    pu_expr = '"PULocationID"' if "pulocationid" in cols else "CAST(NULL AS INTEGER)"
    do_expr = '"DOLocationID"' if "dolocationid" in cols else "CAST(NULL AS INTEGER)"
    pay_expr = "payment_type" if "payment_type" in cols else "CAST(NULL AS INTEGER)"
    total_expr = "total_amount" if "total_amount" in cols else "CAST(NULL AS DOUBLE)"

    # Passenger count & distance (present, but types vary across months)
    pass_expr = "CAST(passenger_count AS INTEGER)" if "passenger_count" in cols else "CAST(NULL AS INTEGER)"
    dist_expr = "trip_distance" if "trip_distance" in cols else "CAST(NULL AS DOUBLE)"

    target = f"{taxi_type}_raw"

    sql = f"""
        INSERT INTO {target}
        SELECT
            {pickup_expr}   AS pickup_datetime,
            {dropoff_expr}  AS dropoff_datetime,
            {pass_expr}     AS passenger_count,
            {dist_expr}     AS trip_distance,
            {pu_expr}       AS pu_location_id,
            {do_expr}       AS do_location_id,
            {pay_expr}      AS payment_type,
            {total_expr}    AS total_amount
        FROM read_parquet(?, union_by_name=true)
    """
    con.execute(sql, [files])


# ---------- NEW: richer descriptive stats ----------
def print_raw_stats(con: duckdb.DuckDBPyConnection) -> None:
    """
    Print/log richer pre-cleaning stats per raw table:
      - total rows
      - avg/median passenger_count
      - avg/median/min/max trip_distance
      - first/last pickup_datetime
    """
    def summarize(table: str) -> dict:
        return con.execute(f"""
            SELECT
                COUNT(*)                                  AS rows,
                AVG(passenger_count)                      AS avg_pax,
                MEDIAN(passenger_count)                   AS med_pax,
                AVG(trip_distance)                        AS avg_miles,
                MEDIAN(trip_distance)                     AS med_miles,
                MIN(trip_distance)                        AS min_miles,
                MAX(trip_distance)                        AS max_miles,
                MIN(pickup_datetime)                      AS first_pickup,
                MAX(pickup_datetime)                      AS last_pickup
            FROM {table}
        """).fetchone()

    for table, label in (("yellow_raw", "yellow_raw"), ("green_raw", "green_raw ")):
        try:
            r = summarize(table)
            (rows, avg_pax, med_pax, avg_mi, med_mi, min_mi, max_mi, first_ts, last_ts) = r
            msg_lines = [
                f"{label} raw row count: {rows:,}",
                f"{label} passenger_count: avg={avg_pax:.2f} | median={med_pax:.2f}",
                f"{label} trip_distance (mi): avg={avg_mi:.3f} | median={med_mi:.3f} | min={min_mi:.3f} | max={max_mi:.3f}",
                f"{label} pickup range: {first_ts} → {last_ts}",
            ]
            for line in msg_lines:
                print(line)
                log.info(line)
        except Exception as e:
            print(f"Could not compute stats for {table}: {e}")
            log.warning("Could not compute stats for %s: %s", table, e)


# ------------------ main ------------------
def main() -> None:
    """End-to-end: ensure folders, download all years, load emissions, create tables, insert normalized rows, print counts+stats."""
    ensure_dirs()

    if not VEHICLE_EMISSIONS_CSV.exists():
        raise FileNotFoundError(
            f"Missing {VEHICLE_EMISSIONS_CSV}. Place vehicle_emissions.csv under ./data/"
        )

    # Download all months for all years/types (skips missing with warnings)
    for t in ("yellow", "green"):
        for y in YEARS:
            for m in MONTHS:
                url, dest = url_and_dest(t, y, m)
                ok = download_with_retries(url, dest)
                if not ok:
                    # Not fatal — older years occasionally lack some months/files
                    log.warning(f"Could not obtain {dest.name} — continuing.")

    con = duckdb.connect(str(DB_PATH), read_only=False)

    load_vehicle_emissions(con)
    create_raw_tables(con)
    insert_trips(con, "yellow")
    insert_trips(con, "green")

    # Print richer stats (replaces the old count-only printout)
    print_raw_stats(con)

    con.close()
    log.info("Load complete.")


if __name__ == "__main__":
    main()