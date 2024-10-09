import os
import yaml
import logging
from utils.db_utils import get_db_engine
from utils.file_utils import read_csv_file
from utils.logger import setup_logger

def load_data(config_path='config/config.yaml'):
    """
    Loads processed data into the PostgreSQL database.

    Args:
        config_path (str): Path to the YAML configuration file.

    Returns:
        None
    """
    logger = setup_logger()
    logger.info("Starting data loading process.")

    # Load configuration
    if not os.path.exists(config_path):
        logger.error(f"Configuration file not found at {config_path}")
        raise FileNotFoundError(f"Configuration file not found at {config_path}")

    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    # Get processed data paths
    processed_data_dir = config.get('paths', {}).get('processed_data')
    if not processed_data_dir:
        logger.error("Processed data directory not specified in the configuration.")
        raise KeyError("Processed data directory not specified in the configuration.")

    # Define processed data file paths
    processed_files = {
        'train': os.path.join(processed_data_dir, 'cleaned_train.csv'),
        'test': os.path.join(processed_data_dir, 'cleaned_test.csv')
    }

    # Get database engine
    try:
        engine = get_db_engine(config_path=config_path)
        logger.info("Database engine created successfully.")
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        raise

    # Load each processed data file into the corresponding database table
    for key, file_path in processed_files.items():
        if not os.path.exists(file_path):
            logger.warning(f"Processed {key} file not found at {file_path}. Skipping.")
            continue

        logger.info(f"Loading {key} data from {file_path} into the database.")
        try:
            df = read_csv_file(file_path)
            table_name = f"nyc_taxi_trip_duration_{key}"
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            logger.info(f"Successfully loaded {key} data into table '{table_name}'.")
        except Exception as e:
            logger.error(f"Failed to load {key} data into the database: {e}")
            raise

    logger.info("Data loading process completed successfully.")

def main():
    """
    Main function to execute the data loading process.
    """
    try:
        load_data()
    except Exception as e:
        logging.error(f"Data loading failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()