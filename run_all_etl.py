"""
run_all_etl.py

Description:
    Orchestrates the entire ETL pipeline by executing setup, testing, and ETL scripts
    in the correct sequence. This script uses psycopg2 to execute SQL scripts directly
    without relying on the psql command-line tool.
"""

import subprocess
import sys
import os
import logging
from datetime import datetime
import psycopg2
from dotenv import load_dotenv

# ==============================================================================
# Configuration
# ==============================================================================

# Load environment variables from .env file
load_dotenv()

# Superuser Configuration (using rexheprexhepi as the superuser)
SUPERUSER_USER = os.getenv('SUPERUSER_USER', 'rexheprexhepi')
SUPERUSER_PASSWORD = os.getenv('SUPERUSER_PASSWORD', '')  # Leave empty if not required

# Database Configuration for ETL Pipeline
DB_USER = os.getenv('DB_USER', 'airflow')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'airflow')  # Ensure this matches setup_db.sql
DB_NAME = os.getenv('DB_NAME', 'airflow')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')

# Define paths relative to the project root
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, 'scripts')
ETL_DIR = os.path.join(PROJECT_ROOT, 'etl')
TESTS_DIR = os.path.join(PROJECT_ROOT, 'tests')
LOGS_DIR = os.path.join(PROJECT_ROOT, 'logs')

# SQL Scripts
SETUP_DB_SQL = os.path.join(SCRIPTS_DIR, 'setup_db.sql')
SETUP_TABLES_SQL = os.path.join(SCRIPTS_DIR, 'setup_tables.sql')

# ETL Scripts
EXTRACT_SCRIPT = os.path.join(ETL_DIR, 'extract.py')
TRANSFORM_ORCHESTRATOR_SCRIPT = os.path.join(ETL_DIR, 'transform.py')  # Orchestrator
LOAD_SCRIPT = os.path.join(ETL_DIR, 'load.py')

# Testing Directory
TEST_COMMAND = ['pytest', TESTS_DIR]

# ==============================================================================
# Logging Setup
# ==============================================================================

def setup_logging():
    """
    Sets up logging for the ETL pipeline.
    Logs are written to 'logs/run_etl_YYYYMMDD_HHMMSS.log'.
    """
    if not os.path.exists(LOGS_DIR):
        os.makedirs(LOGS_DIR)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(LOGS_DIR, f'run_etl_{timestamp}.log')

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

    logging.info(f"Logging initialized. Log file: {log_file}")

# ==============================================================================
# Utility Functions
# ==============================================================================

def run_sql_script_psycopg2(sql_script_path, user, password, dbname=None):
    """
    Executes a SQL script using psycopg2.

    Args:
        sql_script_path (str): Path to the SQL script.
        user (str): PostgreSQL user to connect as.
        password (str): Password for the PostgreSQL user.
        dbname (str): Database to connect to. If None, defaults to 'DB_NAME'.

    Returns:
        None
    """
    if dbname is None:
        dbname = DB_NAME  # Use the default database if none is specified

    logging.info(f"Executing SQL script: {os.path.basename(sql_script_path)} as user: {user}")
    try:
        with open(sql_script_path, 'r') as file:
            sql_commands = file.read()

        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname=dbname,  
            user=user,
            password=password,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Execute SQL commands
        cursor.execute(sql_commands)

        cursor.close()
        conn.close()

        logging.info(f"Successfully executed {os.path.basename(sql_script_path)}")

    except Exception as e:
        logging.error(f"Failed to execute {os.path.basename(sql_script_path)}: {e}")
        sys.exit(1)

def run_python_script(script_path, env=None):
    """
    Executes a Python script.

    Args:
        script_path (str): Path to the Python script.
        env (dict, optional): Environment variables to set for the subprocess.

    Returns:
        None
    """
    command = [
        sys.executable,  # Ensures the same Python interpreter is used
        script_path
    ]
    description = f"Running Python script: {os.path.basename(script_path)}"
    logging.info(f"Starting: {description}")
    try:
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
            env=env  # Pass the environment variables if needed
        )
        logging.info(f"Completed: {description}")
        logging.debug(f"Output:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed: {description}")
        logging.error(f"Error Output:\n{e.stderr}")
        sys.exit(1)

def run_tests():
    """
    Executes all test scripts using pytest.

    Returns:
        None
    """
    command = TEST_COMMAND
    description = "Running Tests with pytest"
    logging.info(f"Starting: {description}")
    try:
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        logging.info(f"Completed: {description}")
        logging.debug(f"Test Output:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed: {description}")
        logging.error(f"Test Error Output:\n{e.stderr}")
        sys.exit(1)

# ==============================================================================
# Main ETL Orchestration
# ==============================================================================

def main():
    """
    Main function to orchestrate the ETL pipeline.
    """
    setup_logging()

    logging.info("==============================================")
    logging.info("Starting ETL Pipeline Execution")
    logging.info("==============================================")

    # 1. Setup Database (Create 'airflow' user and 'airflow' database)
    if os.path.exists(SETUP_DB_SQL):
        # Use rexheprexhepi as the superuser
        run_sql_script_psycopg2(SETUP_DB_SQL, SUPERUSER_USER, SUPERUSER_PASSWORD, dbname=DB_NAME)
    else:
        logging.error(f"SQL script not found: {SETUP_DB_SQL}")
        sys.exit(1)

    # 2. Setup Tables
    if os.path.exists(SETUP_TABLES_SQL):
        # Connect as 'airflow' user now that it exists
        run_sql_script_psycopg2(SETUP_TABLES_SQL, DB_USER, DB_PASSWORD, dbname=DB_NAME)
    else:
        logging.error(f"SQL script not found: {SETUP_TABLES_SQL}")
        sys.exit(1)

    # 3. Run Tests
    run_tests()

    # 4. Run Extract
    if os.path.exists(EXTRACT_SCRIPT):
        run_python_script(EXTRACT_SCRIPT)
    else:
        logging.error(f"ETL script not found: {EXTRACT_SCRIPT}")
        sys.exit(1)

    # 5. Run Transform Orchestrator (which runs scripts/transform.py)
    if os.path.exists(TRANSFORM_ORCHESTRATOR_SCRIPT):
        run_python_script(TRANSFORM_ORCHESTRATOR_SCRIPT)
    else:
        logging.error(f"ETL orchestrator script not found: {TRANSFORM_ORCHESTRATOR_SCRIPT}")
        sys.exit(1)

    # 6. Run Load
    if os.path.exists(LOAD_SCRIPT):
        run_python_script(LOAD_SCRIPT)
    else:
        logging.error(f"ETL script not found: {LOAD_SCRIPT}")
        sys.exit(1)

    logging.info("==============================================")
    logging.info("ETL Pipeline Execution Completed Successfully")
    logging.info("==============================================")

if __name__ == "__main__":
    main()