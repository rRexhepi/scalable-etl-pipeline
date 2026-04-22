"""Unit tests for etl.load.load_data.

We mock the config, filesystem, and SQLAlchemy engine so the test runs
without a real Postgres. It pins the behaviour we care about: loader
reads from the configured processed directory and calls `to_sql` with
the right table names.
"""

import os
from unittest import mock

import pandas as pd
import pytest
import yaml

from etl.load import load_data

_MOCK_CONFIG = {
    "database": {
        "host": "localhost",
        "port": 5432,
        "user": "airflow",
        "password": "airflow",
        "dbname": "airflow",
    },
    "paths": {"processed_data": "data/processed/"},
    "logging": {"level": "INFO", "file": "logs/etl.log"},
}


@pytest.fixture
def fake_config(monkeypatch):
    monkeypatch.setattr(yaml, "safe_load", lambda _: _MOCK_CONFIG)
    monkeypatch.setattr(os.path, "exists", lambda _path: True)


def test_load_data_calls_to_sql_per_dataset(fake_config, monkeypatch):
    monkeypatch.setattr(
        "etl.load.read_csv_file",
        lambda _path: pd.DataFrame({"col": [1, 2, 3]}),
    )
    monkeypatch.setattr(
        "etl.load.get_db_engine",
        lambda config_path: mock.MagicMock(),
    )
    to_sql = mock.MagicMock()
    monkeypatch.setattr(pd.DataFrame, "to_sql", to_sql)

    load_data()

    assert to_sql.call_count == 2
    table_names = {call.kwargs.get("name") or call.args[0] for call in to_sql.call_args_list}
    assert table_names == {"nyc_taxi_trip_duration_train", "nyc_taxi_trip_duration_test"}


def test_load_data_raises_when_read_fails(fake_config, monkeypatch):
    monkeypatch.setattr(
        "etl.load.get_db_engine",
        lambda config_path: mock.MagicMock(),
    )

    def boom(_path):
        raise OSError("disk on fire")

    monkeypatch.setattr("utils.file_utils.read_csv_file", boom)

    with pytest.raises(OSError):
        load_data()
