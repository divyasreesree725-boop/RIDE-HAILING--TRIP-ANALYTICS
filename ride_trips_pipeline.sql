-- Ride-Hailing Trip Analytics
-- Lakeflow Spark Declarative Pipeline (SQL)
-- Default catalog: mobility_catalog   Default schema: trips
-- Source: /Volumes/mobility_catalog/trips/raw_files/ride_trips_raw.csv

-- =========================
-- BRONZE: raw data as-is
-- =========================
CREATE OR REFRESH STREAMING TABLE bronze_ride_trips AS
SELECT *,
       _metadata.file_name AS source_file,
       current_timestamp() AS loaded_at
FROM STREAM read_files('/Volumes/mobility_catalog/trips/raw_files/',
                       format => 'csv', header => true);

-- =========================
-- SILVER: clean + rejected
-- =========================
CREATE OR REFRESH MATERIALIZED VIEW silver_trips_clean (
  CONSTRAINT valid_distance EXPECT (distance_km > 0)  ON VIOLATION DROP ROW,
  CONSTRAINT valid_rate     EXPECT (rate_per_km >= 0) ON VIOLATION DROP ROW
) AS
SELECT DISTINCT
  trip_id, customer_id, driver_id, trip_date, pickup_hour,
  COALESCE(INITCAP(TRIM(city)), 'Unknown')            AS city,
  pickup_zone, region,
  INITCAP(TRIM(vehicle_type))                         AS vehicle_type,
  INITCAP(TRIM(booking_channel))                      AS booking_channel,
  distance_km, duration_min, rate_per_km,
  COALESCE(fare_amount, ROUND(distance_km * rate_per_km, 2)) AS fare_amount,
  discount_amount,
  COALESCE(payment_method, 'Unknown')                 AS payment_method,
  payment_status, driver_payout, customer_rating, trip_type
FROM bronze_ride_trips;

CREATE OR REFRESH MATERIALIZED VIEW silver_trips_rejected AS
SELECT *,
  CASE WHEN distance_km <= 0 THEN 'Distance is zero or negative'
       ELSE 'Rate per km is negative' END AS reject_reason
FROM bronze_ride_trips
WHERE distance_km <= 0 OR rate_per_km < 0;

-- =========================
-- GOLD: business summaries
-- =========================
CREATE OR REFRESH MATERIALIZED VIEW gold_monthly_trips AS
SELECT DATE_TRUNC('MONTH', trip_date)             AS trip_month,
       COUNT(*)                                   AS trips,
       SUM(distance_km)                           AS total_km,
       SUM(fare_amount - discount_amount)         AS net_revenue,
       SUM(driver_payout)                         AS driver_payout,
       SUM(fare_amount - discount_amount) - SUM(driver_payout) AS platform_earnings
FROM silver_trips_clean
GROUP BY DATE_TRUNC('MONTH', trip_date);

CREATE OR REFRESH MATERIALIZED VIEW gold_city_summary AS
SELECT region, city,
       COUNT(*)                                   AS trips,
       SUM(fare_amount - discount_amount)         AS net_revenue,
       ROUND(AVG(fare_amount - discount_amount), 2) AS avg_fare_per_trip
FROM silver_trips_clean
GROUP BY region, city;

CREATE OR REFRESH MATERIALIZED VIEW gold_vehicle_summary AS
SELECT vehicle_type,
       COUNT(*)                                   AS trips,
       SUM(distance_km)                           AS total_km,
       SUM(fare_amount - discount_amount)         AS net_revenue,
       ROUND(AVG(customer_rating), 2)             AS avg_rating
FROM silver_trips_clean
GROUP BY vehicle_type;

CREATE OR REFRESH MATERIALIZED VIEW gold_payment_summary AS
SELECT payment_method,
       COUNT(*)                                   AS trips,
       SUM(fare_amount - discount_amount)         AS net_revenue,
       SUM(CASE WHEN payment_status = 'Pending'
                THEN fare_amount - discount_amount ELSE 0 END) AS pending_amount
FROM silver_trips_clean
GROUP BY payment_method;

-- =========================
-- GOLD: data quality + reconciliation
-- =========================
CREATE OR REFRESH MATERIALIZED VIEW gold_data_quality AS
WITH counts AS (
  SELECT COUNT(*) AS bronze_rows,
         COUNT(*) - COUNT(DISTINCT trip_id) AS duplicate_rows,
         SUM(CASE WHEN distance_km <= 0 THEN 1 ELSE 0 END)        AS invalid_distance_rows,
         SUM(CASE WHEN rate_per_km < 0 THEN 1 ELSE 0 END)         AS negative_rate_rows,
         SUM(CASE WHEN city IS NULL THEN 1 ELSE 0 END)            AS missing_city_rows,
         SUM(CASE WHEN payment_method IS NULL THEN 1 ELSE 0 END)  AS missing_payment_rows,
         SUM(CASE WHEN fare_amount IS NULL THEN 1 ELSE 0 END)     AS missing_fare_rows,
         SUM(CASE WHEN customer_rating IS NULL THEN 1 ELSE 0 END) AS missing_rating_rows,
         SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)     AS missing_customer_rows
  FROM bronze_ride_trips
),
totals AS (
  SELECT c.*,
         (SELECT COUNT(*) FROM silver_trips_clean)    AS clean_rows,
         (SELECT COUNT(*) FROM silver_trips_rejected) AS rejected_rows
  FROM counts c
)
SELECT *,
       CASE WHEN bronze_rows = clean_rows + rejected_rows + duplicate_rows
            THEN 'Yes' ELSE 'No' END AS reconciliation_passed
FROM totals;
