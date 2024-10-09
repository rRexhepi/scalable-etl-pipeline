import pandas as pd
import glob
import yaml
import os

def get_raw_file_paths(config_path='config/config.yaml'):
    """
    Retrieves a list of file paths for raw data based on the configuration.

    Args:
        config_path (str): Path to the YAML configuration file.

    Returns:
        list: List of file paths matching the raw data pattern.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at {config_path}")
    
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    raw_data_pattern = config.get('paths', {}).get('raw_data')
    if not raw_data_pattern:
        raise KeyError("Raw data path pattern not found in the config file under 'paths.raw_data'.")
    
    file_paths = glob.glob(raw_data_pattern)
    if not file_paths:
        raise FileNotFoundError(f"No files found matching the pattern: {raw_data_pattern}")
    
    return file_paths

def read_csv_file(file_path):
    """
    Reads a CSV file into a pandas DataFrame.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        pandas.DataFrame: DataFrame containing the CSV data.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found at {file_path}")
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        raise IOError(f"Error reading CSV file at {file_path}: {e}")
    
    return df

def save_processed_data(df, config_path='config/config.yaml'):
    """
    Saves a pandas DataFrame to a CSV file based on the configuration.

    Args:
        df (pandas.DataFrame): DataFrame to be saved.
        config_path (str): Path to the YAML configuration file.

    Returns:
        str: Path where the processed data was saved.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at {config_path}")
    
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    processed_data_path = config.get('paths', {}).get('processed_data')
    if not processed_data_path:
        raise KeyError("Processed data path not found in the config file under 'paths.processed_data'.")
    
    processed_dir = os.path.dirname(processed_data_path)
    os.makedirs(processed_dir, exist_ok=True)
    
    try:
        df.to_csv(processed_data_path, index=False)
    except Exception as e:
        raise IOError(f"Error saving processed data to {processed_data_path}: {e}")
    
    return processed_data_path