"""Unit tests for the data-quality schemas.

The existing Spark transform is hard to unit-test without a running Spark,
so instead we pin the contract: what 'good' raw and cleaned data look like.
Regressions in the schema (a renamed column, a loosened constraint) will
fail CI.
"""

import pandas as pd
import pandera as pa
import pytest

from etl.validation import CleanedTaxiSchema, RawTaxiSchema, validate


def _good_raw_row():
    return {
        "id": "id1",
        "vendor_id": 1,
        "pickup_datetime": "2016-01-01 00:00:00",
        "dropoff_datetime": "2016-01-01 00:10:00",
        "passenger_count": 1,
        "pickup_longitude": -73.98,
        "pickup_latitude": 40.75,
        "dropoff_longitude": -73.97,
        "dropoff_latitude": 40.76,
        "store_and_fwd_flag": "N",
        "trip_duration": 600,
    }


def test_raw_schema_accepts_valid_rows():
    df = pd.DataFrame([_good_raw_row(), {**_good_raw_row(), "id": "id2"}])
    result = validate(df, RawTaxiSchema, sample=None)
    assert len(result) == 2


def test_raw_schema_rejects_bad_coordinates():
    bad = _good_raw_row()
    bad["pickup_latitude"] = 0.0  # off the equator, not NYC
    df = pd.DataFrame([bad])
    with pytest.raises(pa.errors.SchemaErrors):
        validate(df, RawTaxiSchema, sample=None)


def test_raw_schema_rejects_bad_vendor():
    bad = _good_raw_row()
    bad["vendor_id"] = 99
    df = pd.DataFrame([bad])
    with pytest.raises(pa.errors.SchemaErrors):
        validate(df, RawTaxiSchema, sample=None)


def test_cleaned_schema_rejects_zero_duration():
    df = pd.DataFrame(
        [{"id": "id1", "trip_duration_minutes": 0.0, "passenger_count": 1}]
    )
    with pytest.raises(pa.errors.SchemaErrors):
        validate(df, CleanedTaxiSchema, sample=None)


def test_cleaned_schema_rejects_duration_over_five_hours():
    df = pd.DataFrame(
        [{"id": "id1", "trip_duration_minutes": 301.0, "passenger_count": 1}]
    )
    with pytest.raises(pa.errors.SchemaErrors):
        validate(df, CleanedTaxiSchema, sample=None)


def test_cleaned_schema_accepts_normal_trip():
    df = pd.DataFrame(
        [{"id": "id1", "trip_duration_minutes": 15.5, "passenger_count": 2}]
    )
    result = validate(df, CleanedTaxiSchema, sample=None)
    assert len(result) == 1
