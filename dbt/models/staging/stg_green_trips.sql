{{ config(materialized='table') }}

with factors as (
  select
    avg(case when lower(vehicle_type) like '%green%' then co2_grams_per_mile end) as g_gpm,
    avg(co2_grams_per_mile) as default_gpm
  from {{ source('nyc_taxi', 'vehicle_emissions') }}
)

select
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance,
  pu_location_id,
  do_location_id,
  total_amount,
  'green' as cab_color,

  -- CO₂ (kg): distance * grams_per_mile / 1000, fallback to overall avg if needed
  round(
    trip_distance *
    coalesce((select g_gpm from factors), (select default_gpm from factors)) / 1000.0,
    6
  ) as trip_co2_kgs,

  -- Average MPH
  case
    when (epoch(dropoff_datetime) - epoch(pickup_datetime)) > 0
      then trip_distance / ((epoch(dropoff_datetime) - epoch(pickup_datetime)) / 3600.0)
  end as avg_mph,

  -- Time buckets
  date_part('hour', pickup_datetime) as hour_of_day,
  dayofweek(pickup_datetime) as day_of_week,
  date_part('week', pickup_datetime) as week_of_year,
  month(pickup_datetime) as month_of_year

from {{ source('nyc_taxi', 'green_clean') }}
where pickup_datetime >= timestamp '2015-01-01'
  and pickup_datetime <  timestamp '2025-01-01'