-- Schema for the cleaned NYC Taxi tables written by etl/load.py.
--
-- The column names, order, and types must match what scripts/transform.py
-- produces in data/processed/cleaned_*.csv. The loader COPYs into a session-
-- temp staging table (LIKE this target INCLUDING DEFAULTS) and upserts by
-- primary key, so these tables MUST have a real PK for ON CONFLICT to work.
--
-- Usage:
--   psql -U airflow -d airflow -f scripts/setup_tables.sql

-- ==============================================================================
-- Cleaned train: Kaggle raw columns passed through + trip_duration_minutes.
-- `id` is the Kaggle row identifier and is globally unique; it's the natural
-- conflict key for idempotent upserts.
-- ==============================================================================
CREATE TABLE IF NOT EXISTS nyc_taxi_trip_duration_train (
    id                      TEXT PRIMARY KEY,
    vendor_id               INTEGER,
    pickup_datetime         TIMESTAMP NOT NULL,
    dropoff_datetime        TIMESTAMP,
    passenger_count         INTEGER,
    pickup_longitude        DOUBLE PRECISION,
    pickup_latitude         DOUBLE PRECISION,
    dropoff_longitude       DOUBLE PRECISION,
    dropoff_latitude        DOUBLE PRECISION,
    store_and_fwd_flag      TEXT,
    trip_duration           INTEGER,
    trip_duration_minutes   DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS nyc_taxi_trip_duration_test (
    id                      TEXT PRIMARY KEY,
    vendor_id               INTEGER,
    pickup_datetime         TIMESTAMP NOT NULL,
    dropoff_datetime        TIMESTAMP,
    passenger_count         INTEGER,
    pickup_longitude        DOUBLE PRECISION,
    pickup_latitude         DOUBLE PRECISION,
    dropoff_longitude       DOUBLE PRECISION,
    dropoff_latitude        DOUBLE PRECISION,
    store_and_fwd_flag      TEXT,
    trip_duration           INTEGER,
    trip_duration_minutes   DOUBLE PRECISION
);

-- Watermark lookups (SELECT MAX(pickup_datetime) ...) hit this index.
CREATE INDEX IF NOT EXISTS idx_train_pickup_datetime
    ON nyc_taxi_trip_duration_train (pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_train_dropoff_datetime
    ON nyc_taxi_trip_duration_train (dropoff_datetime);
CREATE INDEX IF NOT EXISTS idx_test_pickup_datetime
    ON nyc_taxi_trip_duration_test (pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_test_dropoff_datetime
    ON nyc_taxi_trip_duration_test (dropoff_datetime);
