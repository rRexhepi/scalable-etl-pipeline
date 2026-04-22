"""Unit tests for the COPY + ON CONFLICT loader.

We mock the SQLAlchemy engine, filesystem, and the ``copy_upsert`` call so
the test runs without a real Postgres. The behaviour we pin:

* The loader reads from the configured processed-data directory.
* It calls ``copy_upsert`` once per dataset with the right target table
  and the ``id`` column as the conflict key.
* When a watermark is present, inputs are filtered by ``pickup_datetime``
  before being upserted.
"""

from __future__ import annotations

import os
from datetime import datetime
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


def _make_df(rows: int = 3) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [f"id{i}" for i in range(rows)],
            "vendor_id": [1] * rows,
            "pickup_datetime": pd.to_datetime(
                [f"2016-03-14 17:{i:02d}:00" for i in range(rows)]
            ),
            "dropoff_datetime": pd.to_datetime(
                [f"2016-03-14 17:{i + 5:02d}:00" for i in range(rows)]
            ),
            "passenger_count": [1] * rows,
            "trip_duration_minutes": [5.0] * rows,
        }
    )


@pytest.fixture
def fake_config(monkeypatch):
    monkeypatch.setattr(yaml, "safe_load", lambda _: _MOCK_CONFIG)
    monkeypatch.setattr(os.path, "exists", lambda _path: True)


def test_load_data_calls_copy_upsert_per_dataset(fake_config, monkeypatch):
    monkeypatch.setattr("etl.load._read_cleaned_csv", lambda _path: _make_df())
    monkeypatch.setattr("etl.load.get_db_engine", lambda config_path: mock.MagicMock())
    # No prior watermark => load everything.
    monkeypatch.setattr("etl.load.get_watermark", lambda engine, table, column: None)

    upsert = mock.MagicMock(return_value=3)
    monkeypatch.setattr("etl.load.copy_upsert", upsert)

    result = load_data()

    assert upsert.call_count == 2
    target_tables = {call.kwargs["target_table"] for call in upsert.call_args_list}
    assert target_tables == {
        "nyc_taxi_trip_duration_train",
        "nyc_taxi_trip_duration_test",
    }
    for call in upsert.call_args_list:
        assert call.kwargs["conflict_columns"] == ["id"]
    assert result == {"train": 3, "test": 3}


def test_load_data_applies_watermark(fake_config, monkeypatch):
    df = _make_df(rows=5)
    monkeypatch.setattr("etl.load._read_cleaned_csv", lambda _path: df.copy())
    monkeypatch.setattr("etl.load.get_db_engine", lambda config_path: mock.MagicMock())

    # Watermark is later than some rows: expect a subset to be upserted.
    watermark = datetime(2016, 3, 14, 17, 2, 0)
    monkeypatch.setattr(
        "etl.load.get_watermark", lambda engine, table, column: watermark
    )

    captured = {}

    def _fake_upsert(engine, df, **kwargs):
        captured.setdefault(kwargs["target_table"], []).append(df.copy())
        return len(df)

    monkeypatch.setattr("etl.load.copy_upsert", _fake_upsert)

    result = load_data()

    # Rows 0..2 have pickup times <= watermark; rows 3,4 survive the filter.
    assert result == {"train": 2, "test": 2}
    for frames in captured.values():
        assert len(frames) == 1
        frame = frames[0]
        assert len(frame) == 2
        assert (frame["pickup_datetime"] > pd.Timestamp(watermark)).all()


def test_load_data_no_watermark_flag_loads_everything(fake_config, monkeypatch):
    df = _make_df(rows=4)
    monkeypatch.setattr("etl.load._read_cleaned_csv", lambda _path: df.copy())
    monkeypatch.setattr("etl.load.get_db_engine", lambda config_path: mock.MagicMock())

    # If the loader honours --no-watermark, we should never consult the DB.
    def _should_not_be_called(*_args, **_kwargs):
        raise AssertionError("get_watermark should not be called when use_watermark=False")

    monkeypatch.setattr("etl.load.get_watermark", _should_not_be_called)

    upsert = mock.MagicMock(side_effect=lambda engine, df, **_kw: len(df))
    monkeypatch.setattr("etl.load.copy_upsert", upsert)

    result = load_data(use_watermark=False)

    assert result == {"train": 4, "test": 4}


def test_load_data_raises_when_read_fails(fake_config, monkeypatch):
    monkeypatch.setattr("etl.load.get_db_engine", lambda config_path: mock.MagicMock())

    def boom(_path):
        raise OSError("disk on fire")

    monkeypatch.setattr("etl.load._read_cleaned_csv", boom)

    with pytest.raises(OSError):
        load_data()
