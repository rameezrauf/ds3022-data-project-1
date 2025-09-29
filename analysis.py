#!/usr/bin/env python3
"""
analysis.py
-----------
Runs rubric-required analyses over NYC taxi data (2015–2024) and saves artifacts.

- Works with either *_trips (preferred) or *_clean.
- Prints & logs the largest CO2 trip and the most/least carbon-heavy hour/day/week/month.
- Constrains every query to 2015–2024.
- Saves CSV artifacts + a 120-point monthly time-series plot (2015-01 .. 2024-12).
"""

import os
import csv
import logging
import duckdb
import matplotlib.pyplot as plt
from typing import Optional, Iterable
from datetime import date

# ---------------- paths, constants & logging ----------------
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_DIR  = os.path.join(BASE_DIR, "data")
DB_PATH   = os.path.join(DATA_DIR, "nyc_taxi.duckdb")
LOGS_DIR  = os.path.join(BASE_DIR, "logs")
PLOTS_DIR = os.path.join(BASE_DIR, "plots")
OUT_DIR   = os.path.join(DATA_DIR, "outputs")  # CSV artifacts for grading
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(PLOTS_DIR, exist_ok=True)
os.makedirs(OUT_DIR,  exist_ok=True)

YEAR_START = "2015-01-01"
YEAR_END   = "2025-01-01"  # exclusive

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=os.path.join(LOGS_DIR, "analysis.log"),
    filemode="w",
)
logger = logging.getLogger(__name__)

# ---------------- small utilities ----------------
def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {name} LIMIT 1")
        return True
    except Exception:
        return False

def columns_of(con: duckdb.DuckDBPyConnection, table: str) -> set:
    return {r[1].lower() for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()}

def pick_source(con: duckdb.DuckDBPyConnection, color: str) -> Optional[str]:
    for t in (f"{color}_trips", f"{color}_clean"):
        if table_exists(con, t):
            return t
    return None

def safe_exec_one(con: duckdb.DuckDBPyConnection, sql: str):
    try:
        return con.execute(sql).fetchone()
    except Exception as e:
        logger.error("Query failed: %s\nSQL:\n%s", e, sql)
        return None

def safe_exec_all(con: duckdb.DuckDBPyConnection, sql: str):
    try:
        return con.execute(sql).fetchall()
    except Exception as e:
        logger.error("Query failed: %s\nSQL:\n%s", e, sql)
        return []

def write_csv(path: str, rows: Iterable[Iterable], header: Iterable[str]) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(r)

# ---------------- schema-agnostic timestamp exprs ----------------
def pickup_expr(cols: set) -> str:
    return "pickup_datetime" if "pickup_datetime" in cols else "COALESCE(tpep_pickup_datetime, lpep_pickup_datetime)"

def dropoff_expr(cols: set) -> str:
    return "dropoff_datetime" if "dropoff_datetime" in cols else "COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime)"

# ---------------- main analysis ----------------
def analysis() -> None:
    con = None
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        logger.info("Connected to DuckDB for analysis.")
        print(f"Connected to DuckDB for analysis at: {DB_PATH}")

        # Resolve sources
        sources = {c: pick_source(con, c) for c in ("yellow", "green")}
        for c, s in sources.items():
            if s is None:
                msg = f"No source table found for {c.upper()} (looked for {c}_trips, {c}_clean)."
                print(msg); logger.warning(msg)

        # -------- 1) Largest carbon producing trip --------
        print("\n--- Largest Carbon Producing Trips (2015–2024) ---")
        top_rows_for_csv = []
        for color in ("yellow", "green"):
            src = sources[color]
            if not src:
                print(f"{color.upper()}: no source table available."); continue

            cols = columns_of(con, src)
            if "trip_co2_kgs" not in cols:
                print(f"{color.upper()}: trip_co2_kgs not present in {src}; skipping.")
                logger.info("%s: trip_co2_kgs not present in %s", color, src)
                continue

            dur_hr = f"(EPOCH({dropoff_expr(cols)}) - EPOCH({pickup_expr(cols)})) / 3600.0"
            avg_mph_sql = "avg_mph" if "avg_mph" in cols else f"CASE WHEN {dur_hr} > 0 THEN trip_distance / ({dur_hr}) END"

            sql = f"""
                SELECT
                    '{color.upper()}' AS taxi_type,
                    {pickup_expr(cols)} AS pickup_ts,
                    {dropoff_expr(cols)} AS dropoff_ts,
                    trip_distance,
                    COALESCE(passenger_count, 0) AS passenger_count,
                    trip_co2_kgs,
                    {avg_mph_sql} AS avg_mph_safe
                FROM {src}
                WHERE trip_co2_kgs IS NOT NULL
                  AND {pickup_expr(cols)} >= TIMESTAMP '{YEAR_START}'
                  AND {pickup_expr(cols)} <  TIMESTAMP '{YEAR_END}'
                ORDER BY trip_co2_kgs DESC
                LIMIT 1
            """
            row = safe_exec_one(con, sql)
            if row:
                taxi_type, pu, do, dist, pax, co2, mph = row
                if mph is not None:
                    print(f"{taxi_type}: Distance {dist:.2f} mi, Pax {int(pax)}, CO₂ {co2:.3f} kg, Avg MPH {mph:.2f}")
                else:
                    print(f"{taxi_type}: Distance {dist:.2f} mi, Pax {int(pax)}, CO₂ {co2:.3f} kg")
                logger.info("%s top trip: %s", taxi_type, row)
                top_rows_for_csv.append(row)

        if top_rows_for_csv:
            write_csv(
                os.path.join(OUT_DIR, "largest_co2_trip.csv"),
                top_rows_for_csv,
                header=["taxi_type", "pickup_ts", "dropoff_ts", "trip_distance", "passenger_count", "trip_co2_kgs", "avg_mph"]
            )

        # -------- 2–5) Most/least carbon heavy hour/day/week/month --------
        time_buckets = {
            "Hour of the Day":  ("hour_of_day",  lambda p: f"date_part('hour', {p})"),
            "Day of the Week":  ("day_of_week",  lambda p: f"dayofweek({p})"),
            "Week of the Year": ("week_of_year", lambda p: f"date_part('week', {p})"),
            "Month of the Year":("month_of_year",lambda p: f"month({p})"),
        }

        for label, (precol, fallback_fn) in time_buckets.items():
            print(f"\n--- Carbon Heavy/Light {label} (2015–2024) ---")
            all_bucket_rows = []
            for color in ("yellow", "green"):
                src = sources[color]
                if not src:
                    print(f"{color.upper()}: no source table available."); continue

                cols = columns_of(con, src)
                if "trip_co2_kgs" not in cols:
                    print(f"{color.upper()}: No trip_co2_kgs in {src}."); continue

                bucket_expr = precol if precol in cols else fallback_fn(pickup_expr(cols))
                sql = f"""
                    SELECT
                        '{color.upper()}' AS taxi_type,
                        {bucket_expr} AS bucket,
                        AVG(trip_co2_kgs) AS avg_co2,
                        COUNT(*) AS trips
                    FROM {src}
                    WHERE trip_co2_kgs IS NOT NULL
                      AND {pickup_expr(cols)} >= TIMESTAMP '{YEAR_START}'
                      AND {pickup_expr(cols)} <  TIMESTAMP '{YEAR_END}'
                    GROUP BY 1, 2
                    HAVING COUNT(*) > 0
                    ORDER BY avg_co2 DESC
                """
                rows = safe_exec_all(con, sql)
                if rows:
                    hi, lo = rows[0], rows[-1]
                    print(f"{color.upper()}:")
                    print(f"  - Most Carbon Heavy -> {label.split(' ')[0]} {int(hi[1])}: {hi[2]:.4f} kg/trip (n={hi[3]:,})")
                    print(f"  - Most Carbon Light -> {label.split(' ')[0]} {int(lo[1])}: {lo[2]:.4f} kg/trip (n={lo[3]:,})")
                    logger.info("%s %s heaviest=%s lightest=%s", color.upper(), label, hi, lo)
                    all_bucket_rows.extend(rows)
                else:
                    print(f"{color.upper()}: No data for {label.lower()} (2015–2024).")

            if all_bucket_rows:
                out_name = f"buckets_{precol}.csv"
                write_csv(
                    os.path.join(OUT_DIR, out_name),
                    all_bucket_rows,
                    header=["taxi_type", "bucket", "avg_co2", "trips"]
                )

        # -------- 6) TRUE monthly time series (120 points, not 12 buckets) --------
        print("\n--- Generating Monthly CO₂ Totals Time Series (2015–01 .. 2024–12) ---")

        def month_series(con: duckdb.DuckDBPyConnection, src: str) -> list[tuple[str, float]]:
            """
            Returns [(month_start_iso, total_kg)] where month_start_iso is YYYY-MM-01,
            aggregated by the actual month (not just month_of_year).
            """
            cols = columns_of(con, src)
            month_key = f"date_trunc('month', {pickup_expr(cols)})"
            sql = f"""
                SELECT
                    CAST({month_key} AS DATE) AS month_start,
                    SUM(trip_co2_kgs) AS total_kg
                FROM {src}
                WHERE trip_co2_kgs IS NOT NULL
                  AND {pickup_expr(cols)} >= TIMESTAMP '{YEAR_START}'
                  AND {pickup_expr(cols)} <  TIMESTAMP '{YEAR_END}'
                GROUP BY 1
                ORDER BY 1
            """
            rows = safe_exec_all(con, sql)
            # Convert to ISO strings for easy CSV & keys
            return [(r[0].isoformat(), float(r[1])) for r in rows]

        y_src, g_src = sources["yellow"], sources["green"]
        y_rows = month_series(con, y_src) if y_src else []
        g_rows = month_series(con, g_src) if g_src else []

        # Save per-month totals for grading (full 120-point series)
        mt_rows = [("YELLOW",) + r for r in y_rows] + [("GREEN",) + r for r in g_rows]
        if mt_rows:
            write_csv(
                os.path.join(OUT_DIR, "monthly_totals_co2_by_month.csv"),
                mt_rows,
                header=["taxi_type", "month_start", "total_co2_kg"]
            )

        # Build a complete 2015-01..2024-12 axis and fill missing with zeros
        def month_range(start: date, end_exclusive: date):
            months = []
            y, m = start.year, start.month
            while (y < end_exclusive.year) or (y == end_exclusive.year and m < end_exclusive.month):
                months.append(date(y, m, 1))
                if m == 12:
                    y += 1; m = 1
                else:
                    m += 1
            return months

        all_months = month_range(date(2015, 1, 1), date(2025, 1, 1))
        y_map = {d: 0.0 for d in all_months}
        g_map = {d: 0.0 for d in all_months}

        for iso, val in y_rows:
            y_map[date.fromisoformat(iso)] = val
        for iso, val in g_rows:
            g_map[date.fromisoformat(iso)] = val

        if not any(y_map.values()) and not any(g_map.values()):
            print("No data available to plot monthly CO₂ totals (2015–2024).")
            logger.info("No data available to plot monthly CO₂ totals.")
        else:
            x = all_months
            y_vals = [y_map[d] for d in x]
            g_vals = [g_map[d] for d in x]

            plt.figure(figsize=(13, 6))
            plt.plot(x, y_vals, marker="o", linewidth=1.2, label="Yellow", color="#f6c103")
            plt.plot(x, g_vals, marker="s", linewidth=1.2, label="Green",  color="#2aa84a")
            plt.title("Monthly CO₂ Emissions by Taxi Type (2015–01 .. 2024–12)")
            plt.xlabel("Month")
            plt.ylabel("Total CO₂ (kg)")
            plt.grid(True, which="both", linestyle="--", linewidth=0.5)
            plt.legend()
            # Fewer ticks: show Jan of each year for readability
            yearly_ticks = [date(y, 1, 1) for y in range(2015, 2025)]
            plt.xticks(yearly_ticks, [str(y.year) for y in yearly_ticks], rotation=0)
            out_png = os.path.join(PLOTS_DIR, "monthly_co2_timeseries_2015_2024.png")
            plt.tight_layout()
            plt.savefig(out_png, dpi=150)
            plt.close()
            print(f"Saved plot: {out_png}")
            logger.info("Time-series plot saved: %s", out_png)

        print(f"\nCSV artifacts saved under: {OUT_DIR}")

    except Exception as e:
        logger.exception("An error occurred during analysis: %s", e)
        print(f"An error occurred during analysis: {e}")
    finally:
        if con is not None:
            con.close()

if __name__ == "__main__":
    analysis()