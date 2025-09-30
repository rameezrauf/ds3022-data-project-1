#!/usr/bin/env python3
"""
load.py — minimal, rubric-ready

What it does:
- Downloads NYC Taxi Parquet files for Yellow & Green (2015–2024) to ./data/raw/<type>/<year>/
- Loads ./data/vehicle_emissions.csv (only needed columns) into DuckDB
- Normalizes schema differences and inserts into yellow_raw, green_raw
- Prints basic pre-cleaning stats

Outputs:
- DuckDB at ./data/nyc_taxi.duckdb
- Log at ./logs/load.log
"""

import logging
import time
from pathlib import Path
from typing import List, Tuple

import requests
import duckdb

# ---------------- paths & logging ----------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
DB_PATH = DATA_DIR / "nyc_taxi.duckdb"
VEHICLE_EMISSIONS_CSV = DATA_DIR / "vehicle_emissions.csv"

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_DIR / "load.log"), logging.StreamHandler()],
)
log = logging.getLogger("load")

# ---------------- constants ----------------
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
YEARS = list(range(2015, 2025))           # 2015–2024 inclusive
MONTHS = [f"{m:02d}" for m in range(1, 13)]
USER_AGENT = "Mozilla/5.0"
TIMEOUT = 60
RETRIES = 2
SKIP_EXISTING = True

# ---------------- helpers ----------------
# Create the ./data/raw/<type>/<year>/ folder structure if missing
def ensure_dirs() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    for t in ("yellow", "green"):
        for y in YEARS:
            (RAW_DIR / t / str(y)).mkdir(parents=True, exist_ok=True)

# Build the download URL and the local destination path for a given taxi type/year/month
def url_and_dest(taxi_type: str, year: int, month: str) -> Tuple[str, Path]:
    fname = f"{taxi_type}_tripdata_{year}-{month}.parquet"
    return f"{BASE_URL}/{fname}", RAW_DIR / taxi_type / str(year) / fname

# Download a file with minimal retries; skip if already present (when SKIP_EXISTING=True)
def download_with_retries(url: str, dest: Path) -> bool:
    if SKIP_EXISTING and dest.exists() and dest.stat().st_size > 0:
        log.info(f"Already present, skipping: {dest.name}")
        return True
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(1, RETRIES + 1):
        try:
            with requests.get(url, headers=headers, stream=True, timeout=TIMEOUT) as r:
                if r.status_code == 200:
                    tmp = dest.with_suffix(".part")
                    with open(tmp, "wb") as f:
                        for chunk in r.iter_content(1024 * 1024):
                            if chunk: f.write(chunk)
                    tmp.rename(dest)
                    log.info(f"Downloaded {dest.name}")
                    return True
                log.warning(f"[{r.status_code}] {url}")
        except Exception as e:
            log.warning(f"Attempt {attempt}/{RETRIES} failed for {url}: {e}")
        time.sleep(1.5 * attempt)
    return dest.exists() and dest.stat().st_size > 0

# Collect a list of all local parquet files for a taxi type across all configured years
def local_files(taxi_type: str) -> List[str]:
    files: List[str] = []
    for y in YEARS:
        files += [str(p) for p in sorted((RAW_DIR / taxi_type / str(y)).glob("*.parquet"))]
    return files

# ---------------- load steps ----------------
# Create/refresh the vehicle_emissions lookup table from CSV (only required columns)
def load_vehicle_emissions(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS vehicle_emissions (
            vehicle_type VARCHAR,
            co2_grams_per_mile DOUBLE
        );
        DELETE FROM vehicle_emissions;
    """)
    # Read only the needed columns (ignore extras), cast explicitly
    con.execute("""
        INSERT INTO vehicle_emissions
        SELECT vehicle_type, CAST(co2_grams_per_mile AS DOUBLE)
        FROM read_csv_auto(?, header=True, all_varchar=True)
    """, [str(VEHICLE_EMISSIONS_CSV)])
    n = con.execute("SELECT COUNT(*) FROM vehicle_emissions").fetchone()[0]
    print(f"vehicle_emissions raw row count: {n:,}")
    log.info(f"vehicle_emissions rows: {n:,}")

# Create empty normalized raw tables (yellow_raw, green_raw) and clear any existing rows
def create_raw_tables(con: duckdb.DuckDBPyConnection) -> None:
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

# Read monthly parquet files and insert into target raw table with a stable schema
def insert_trips(con: duckdb.DuckDBPyConnection, taxi_type: str) -> None:
    files = local_files(taxi_type)
    if not files:
        log.warning(f"No local files for {taxi_type}. Skipping insert.")
        return
    # Infer columns available across all files
    desc = con.execute("SELECT * FROM read_parquet(?, union_by_name=true) LIMIT 0", [files]).description
    cols = {c[0].lower() for c in desc}

    if taxi_type == "yellow":
        pickup, dropoff = "tpep_pickup_datetime", "tpep_dropoff_datetime"
        if pickup not in cols or dropoff not in cols:
            raise RuntimeError("Expected tpep_* columns for yellow not found.")
    else:
        pickup, dropoff = "lpep_pickup_datetime", "lpep_dropoff_datetime"
        if pickup not in cols or dropoff not in cols:
            raise RuntimeError("Expected lpep_* columns for green not found.")

    pu_expr = '"PULocationID"' if "pulocationid" in cols else "CAST(NULL AS INTEGER)"
    do_expr = '"DOLocationID"' if "dolocationid" in cols else "CAST(NULL AS INTEGER)"
    pay    = "payment_type" if "payment_type" in cols else "CAST(NULL AS INTEGER)"
    total  = "total_amount"  if "total_amount"  in cols else "CAST(NULL AS DOUBLE)"
    pax    = "CAST(passenger_count AS INTEGER)" if "passenger_count" in cols else "CAST(NULL AS INTEGER)"
    dist   = "trip_distance" if "trip_distance" in cols else "CAST(NULL AS DOUBLE)"

    target = f"{taxi_type}_raw"
    con.execute(f"""
        INSERT INTO {target}
        SELECT
            {pickup}  AS pickup_datetime,
            {dropoff} AS dropoff_datetime,
            {pax}     AS passenger_count,
            {dist}    AS trip_distance,
            {pu_expr} AS pu_location_id,
            {do_expr} AS do_location_id,
            {pay}     AS payment_type,
            {total}   AS total_amount
        FROM read_parquet(?, union_by_name=true)
    """, [files])

# Compute and print concise pre-cleaning stats for each raw table (with simple plausibility filters)
def print_raw_stats(con: duckdb.DuckDBPyConnection) -> None:
    # Helper: summarize one table’s counts and distributions within bounded ranges
    def summarize(table: str):
        return con.execute(f"""
            WITH base AS (
                SELECT *
                FROM {table}
                WHERE pickup_datetime >= '2015-01-01'
                  AND pickup_datetime <  '2025-01-01'
                  AND trip_distance BETWEEN 0 AND 200
                  AND passenger_count BETWEEN 0 AND 8
            )
            SELECT
                COUNT(*)                AS rows,
                AVG(passenger_count)    AS avg_pax,
                MEDIAN(passenger_count) AS med_pax,
                AVG(trip_distance)      AS avg_miles,
                MEDIAN(trip_distance)   AS med_miles,
                MIN(trip_distance)      AS min_miles,
                MAX(trip_distance)      AS max_miles,
                MIN(pickup_datetime)    AS first_pickup,
                MAX(pickup_datetime)    AS last_pickup
            FROM base
        """).fetchone()

    for table in ("yellow_raw", "green_raw"):
        try:
            rows, avg_pax, med_pax, avg_mi, med_mi, min_mi, max_mi, first_ts, last_ts = summarize(table)
            print(f"{table} raw row count: {rows:,}")
            print(f"{table} passenger_count: avg={avg_pax:.2f} | median={med_pax:.2f}")
            print(f"{table} trip_distance (mi): avg={avg_mi:.3f} | median={med_mi:.3f} | min={min_mi:.3f} | max={max_mi:.3f}")
            print(f"{table} pickup range: {first_ts} → {last_ts}")
            log.info(f"{table} rows={rows:,} avg_pax={avg_pax:.2f} med_pax={med_pax:.2f} avg_mi={avg_mi:.3f}")
        except Exception as e:
            log.warning(f"Could not compute stats for {table}: {e}")
            print(f"Could not compute stats for {table}: {e}")

# ---------------- main ----------------
# Orchestrate the end-to-end flow: prep folders, download, load CSV, build tables, insert trips, print stats
def main() -> None:
    ensure_dirs()

    if not VEHICLE_EMISSIONS_CSV.exists():
        raise FileNotFoundError(f"Missing {VEHICLE_EMISSIONS_CSV}. Put vehicle_emissions.csv under ./data/")

    # Download (skip if already present)
    for taxi_type in ("yellow", "green"):
        for y in YEARS:
            for m in MONTHS:
                url, dest = url_and_dest(taxi_type, y, m)
                download_with_retries(url, dest)

    con = duckdb.connect(str(DB_PATH), read_only=False)
    load_vehicle_emissions(con)
    create_raw_tables(con)
    insert_trips(con, "yellow")
    insert_trips(con, "green")
    print_raw_stats(con)
    con.close()
    log.info("Load complete.")

if __name__ == "__main__":
    main()