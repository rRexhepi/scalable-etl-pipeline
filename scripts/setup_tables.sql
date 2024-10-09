-- Description:
--   This script creates the required tables in the 'airflow' database for the ETL
--   pipeline. It defines two tables: 'nyc_taxi_trip_duration_train' and
--   'nyc_taxi_trip_duration_test' with appropriate columns and data types.
--
-- Usage:
--   psql -U rexheprexhepi -d airflow -f scripts/setup_tables.sql
--
-- ==============================================================================

-- ==============================================================================
-- Table: nyc_taxi_trip_duration_train
-- ==============================================================================
CREATE TABLE IF NOT EXISTS nyc_taxi_trip_duration_train (
    VendorID VARCHAR(50),
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance FLOAT,
    pickup_longitude FLOAT,
    pickup_latitude FLOAT,
    dropoff_longitude FLOAT,
    dropoff_latitude FLOAT,
    trip_duration_minutes FLOAT,
    PRIMARY KEY (VendorID, pickup_datetime)
);

-- ==============================================================================
-- Table: nyc_taxi_trip_duration_test
-- ==============================================================================
CREATE TABLE IF NOT EXISTS nyc_taxi_trip_duration_test (
    VendorID VARCHAR(50),
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance FLOAT,
    pickup_longitude FLOAT,
    pickup_latitude FLOAT,
    dropoff_longitude FLOAT,
    dropoff_latitude FLOAT,
    trip_duration_minutes FLOAT,
    PRIMARY KEY (VendorID, pickup_datetime)
);

-- ==============================================================================
-- Indexes for performance optimization
-- ==============================================================================
CREATE INDEX IF NOT EXISTS idx_train_pickup_datetime ON nyc_taxi_trip_duration_train (pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_train_dropoff_datetime ON nyc_taxi_trip_duration_train (dropoff_datetime);
CREATE INDEX IF NOT EXISTS idx_test_pickup_datetime ON nyc_taxi_trip_duration_test (pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_test_dropoff_datetime ON nyc_taxi_trip_duration_test (dropoff_datetime);