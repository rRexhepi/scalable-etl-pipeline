import pytest
from unittest import mock
from etl.load import load_data
from utils.db_utils import get_db_engine
from utils.file_utils import read_csv_file

@pytest.fixture
def mock_config(monkeypatch):
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
    monkeypatch.setattr('builtins.open', mock.mock_open(read_data=yaml.dump(mock_config)))
    monkeypatch.setattr(os.path, 'exists', lambda path: True)
    return mock_config

def test_load_data_success(monkeypatch, mock_config):
    # Mock the database engine
    mock_engine = mock.MagicMock()
    monkeypatch.setattr('utils.db_utils.get_db_engine', lambda config_path: mock_engine)

    # Mock read_csv_file to return a simple DataFrame
    import pandas as pd
    mock_df = pd.DataFrame({'column1': [1, 2, 3]})
    monkeypatch.setattr('utils.file_utils.read_csv_file', lambda file_path: mock_df)

    # Mock to_sql to do nothing
    monkeypatch.setattr(mock_engine, 'execute', lambda *args, **kwargs: None)
    monkeypatch.setattr(mock_engine, 'dispose', lambda: None)
    monkeypatch.setattr(pd.DataFrame, 'to_sql', lambda self, name, con, if_exists, index: None)

    # Run the load_data function
    load_data()

def test_load_data_missing_file(monkeypatch, mock_config):
    # Mock the database engine
    mock_engine = mock.MagicMock()
    monkeypatch.setattr('utils.db_utils.get_db_engine', lambda config_path: mock_engine)

    # Mock read_csv_file to raise FileNotFoundError
    monkeypatch.setattr('utils.file_utils.read_csv_file', lambda file_path: (_ for _ in ()).throw(FileNotFoundError))

    # Expect the load_data function to raise FileNotFoundError
    with pytest.raises(FileNotFoundError):
        load_data()