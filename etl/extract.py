import os
import yaml
import logging
from utils.file_utils import read_csv_file
from utils.logger import setup_logger

def extract_data(config_path='config/config.yaml'):
    """
    Extracts raw data from the specified directory.

    Args:
        config_path (str): Path to the YAML configuration file.

    Returns:
        dict: A dictionary containing DataFrames for each raw data file.
              Example: {'train': train_df, 'test': test_df, 'sample_submission': sample_submission_df}
    """
    logger = setup_logger()
    logger.info("Starting data extraction process.")

    # Load configuration
    if not os.path.exists(config_path):
        logger.error(f"Configuration file not found at {config_path}")
        raise FileNotFoundError(f"Configuration file not found at {config_path}")

    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    raw_data_dir = config.get('paths', {}).get('raw_data')
    if not raw_data_dir:
        logger.error("Raw data directory not specified in the configuration.")
        raise KeyError("Raw data directory not specified in the configuration.")

    # Define raw data file paths
    raw_files = {
        'train': os.path.join(raw_data_dir, 'train.csv'),
        'test': os.path.join(raw_data_dir, 'test.csv'),
        'sample_submission': os.path.join(raw_data_dir, 'sample_submission.csv')
    }

    data_frames = {}

    # Read each raw data file into a DataFrame
    for key, file_path in raw_files.items():
        if not os.path.exists(file_path):
            logger.warning(f"{key.capitalize()} file not found at {file_path}. Skipping.")
            continue

        logger.info(f"Reading {key} data from {file_path}")
        try:
            df = read_csv_file(file_path)
            data_frames[key] = df
            logger.info(f"Successfully read {key} data with {len(df)} records.")
        except Exception as e:
            logger.error(f"Failed to read {key} data from {file_path}: {e}")
            raise

    if not data_frames:
        logger.error("No raw data files were found. Extraction aborted.")
        raise ValueError("No raw data files were found. Extraction aborted.")

    logger.info("Data extraction completed successfully.")
    return data_frames

def main():
    """
    Main function to execute the extraction process.
    """
    try:
        extracted_data = extract_data()
        # Optionally, you can perform further actions with the extracted_data here
        # For example, save them to processed_data or pass to the transform step
    except Exception as e:
        logging.error(f"Data extraction failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()