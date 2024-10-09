-- Description:
--   This script sets up the PostgreSQL database and user required for the ETL
--   pipeline. It creates a database named 'airflow' and a user named 'airflow'
--   with the password 'airflow'. Adjust the names and credentials as needed.
--
-- Usage:
--   psql -U rexheprexhepi -f scripts/setup_db.sql
--
-- ==============================================================================

-- Create user 'airflow' with password 'airflow' if it doesn't exist
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT
      FROM pg_catalog.pg_user
      WHERE usename = 'airflow') THEN

      CREATE USER airflow WITH PASSWORD 'airflow';
      RAISE NOTICE 'User "airflow" created.';
   ELSE
      RAISE NOTICE 'User "airflow" already exists.';
   END IF;
END
$do$;

-- Create database 'airflow' if it doesn't exist and assign ownership to 'airflow' user
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT
      FROM pg_database
      WHERE datname = 'airflow') THEN

      CREATE DATABASE airflow
          WITH 
          OWNER = airflow
          ENCODING = 'UTF8'
          LC_COLLATE = 'en_US.UTF-8'
          LC_CTYPE = 'en_US.UTF-8'
          CONNECTION LIMIT = -1;
      RAISE NOTICE 'Database "airflow" created.';
   ELSE
      RAISE NOTICE 'Database "airflow" already exists.';
   END IF;
END
$do$;