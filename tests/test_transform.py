import pytest
from unittest import mock
from etl.load import load_data
from utils.db_utils import get_db_engine
from utils.file_utils import read_csv_file
import pandas as pd
import os
import yaml

# Sample mock configuration
mock_config = {
    'database': {
        'host': 'localhost',
        'port': 5432,
        'user': 'airflow',
        'password': 'airflow',
        'dbname': 'airflow'
    },
    'paths': {
        'processed_data': 'data/processed/'
    },
    'logging': {
        'level': 'INFO',
        'file': 'logs/etl.log'
    }
}

@pytest.fixture
def mock_yaml_load(monkeypatch):
    """
    Fixture to mock yaml.safe_load to return a predefined configuration.
    """
    def mock_safe_load(file):
        return mock_config

    monkeypatch.setattr(yaml, 'safe_load', mock_safe_load)

@pytest.fixture
def mock_file_exists(monkeypatch):
    """
    Fixture to mock os.path.exists to return True for processed data files.
    """
    def mock_exists(path):
        if 'cleaned_train.csv' in path or 'cleaned_test.csv' in path:
            return True
        return False
    monkeypatch.setattr(os.path, 'exists', mock_exists)

@pytest.fixture
def mock_read_csv_file(monkeypatch):
    """
    Fixture to mock read_csv_file to return a simple DataFrame.
    """
    mock_train_df = pd.DataFrame({
        'VendorID': ['vendor1', 'vendor2'],
        'pickup_datetime': ['2024-01-01 08:00:00', '2024-01-01 09:00:00'],
        'dropoff_datetime': ['2024-01-01 08:30:00', '2024-01-01 09:45:00'],
        'passenger_count': [1, 2],
        'trip_distance': [2.5, 3.1],
        'pickup_longitude': [-73.95, -73.90],
        'pickup_latitude': [40.78, 40.75],
        'dropoff_longitude': [-73.98, -73.95],
        'dropoff_latitude': [40.76, 40.70],
        'trip_duration_minutes': [30.0, 45.0]
    })
    
    mock_test_df = pd.DataFrame({
        'VendorID': ['vendor1'],
        'pickup_datetime': ['2024-01-02 10:00:00'],
        'dropoff_datetime': ['2024-01-02 10:25:00'],
        'passenger_count': [1],
        'trip_distance': [1.8],
        'pickup_longitude': [-73.99],
        'pickup_latitude': [40.73],
        'dropoff_longitude': [-74.00],
        'dropoff_latitude': [40.72],
        'trip_duration_minutes': [25.0]
    })
    
    def mock_read_csv(file_path):
        if 'cleaned_train.csv' in file_path:
            return mock_train_df
        elif 'cleaned_test.csv' in file_path:
            return mock_test_df
        else:
            return pd.DataFrame()
    
    monkeypatch.setattr('utils.file_utils.read_csv_file', mock_read_csv)

@pytest.fixture
def mock_db_engine(monkeypatch):
    """
    Fixture to mock the database engine and its methods.
    """
    mock_engine = mock.MagicMock()
    mock_connection = mock.MagicMock()
    mock_engine.connect.return_value = mock_connection
    monkeypatch.setattr('utils.db_utils.create_engine', lambda url: mock_engine)
    return mock_engine

def test_load_data_success(mock_yaml_load, mock_file_exists, mock_read_csv_file, mock_db_engine, monkeypatch):
    """
    Test that load_data successfully loads data into the database.
    """
    # Mock to_sql to simulate successful data loading
    mock_to_sql = mock.MagicMock()
    monkeypatch.setattr(pd.DataFrame, 'to_sql', mock_to_sql)
    
    load_data()
    
    # Verify that to_sql was called for both train and test datasets
    assert mock_to_sql.call_count == 2
    
    # Verify calls to to_sql with correct parameters
    calls = mock_to_sql.call_args_list
    assert calls[0][1]['name'] == 'nyc_taxi_trip_duration_train'
    assert calls[0][1]['con'] == mock_db_engine
    assert calls[0][1]['if_exists'] == 'replace'
    assert calls[0][1]['index'] == False
    
    assert calls[1][1]['name'] == 'nyc_taxi_trip_duration_test'
    assert calls[1][1]['con'] == mock_db_engine
    assert calls[1][1]['if_exists'] == 'replace'
    assert calls[1][1]['index'] == False

def test_load_data_missing_files(mock_yaml_load, monkeypatch):
    """
    Test that load_data handles missing processed data files gracefully.
    """
    # Mock os.path.exists to return False for all files
    monkeypatch.setattr(os.path, 'exists', lambda path: False)
    
    with pytest.raises(KeyError) as excinfo:
        load_data()
    
    assert "Processed data directory not specified in the config file under 'paths.processed_data'." in str(excinfo.value)

def test_load_data_read_csv_failure(mock_yaml_load, mock_file_exists, monkeypatch):
    """
    Test that load_data raises an error when reading CSV files fails.
    """
    # Mock read_csv_file to raise an IOError
    def mock_read_csv(file_path):
        raise IOError("Failed to read CSV file.")
    
    monkeypatch.setattr('utils.file_utils.read_csv_file', mock_read_csv)
    
    with pytest.raises(IOError) as excinfo:
        load_data()
    
    assert "Failed to load" in str(excinfo.value)