import pytest
from etl.extract import extract_data
import os
import pandas as pd

def test_extract_data_success(monkeypatch):
    # Setup mock config
    mock_config = {
        'paths': {
            'raw_data': 'data/raw/'
        }
    }

    # Mock yaml.safe_load to return mock_config
    def mock_safe_load(file):
        return mock_config

    monkeypatch.setattr('yaml.safe_load', mock_safe_load)

    # Mock os.path.exists to always return True
    monkeypatch.setattr(os.path, 'exists', lambda path: True)

    # Mock read_csv_file to return a simple DataFrame
    def mock_read_csv_file(file_path):
        return pd.DataFrame({'column1': [1, 2, 3]})

    monkeypatch.setattr('utils.file_utils.read_csv_file', mock_read_csv_file)

    extracted_data = extract_data()
    assert 'train' in extracted_data
    assert 'test' in extracted_data
    assert 'sample_submission' in extracted_data
    assert isinstance(extracted_data['train'], pd.DataFrame)
    assert extracted_data['train'].shape == (3, 1)

def test_extract_data_no_files(monkeypatch):
    # Setup mock config
    mock_config = {
        'paths': {
            'raw_data': 'data/raw/'
        }
    }

    # Mock yaml.safe_load to return mock_config
    def mock_safe_load(file):
        return mock_config

    monkeypatch.setattr('yaml.safe_load', mock_safe_load)

    # Mock os.path.exists to return False for all files
    monkeypatch.setattr(os.path, 'exists', lambda path: False)

    with pytest.raises(ValueError):
        extract_data()