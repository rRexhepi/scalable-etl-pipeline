"""Pandera schemas for data-quality gates in the ETL pipeline.

Raw: the NYC Taxi Trip Duration source CSV.
Cleaned: what the Spark transform produces and load.py pushes to Postgres.

Schemas are intentionally strict enough to catch real regressions (wrong
dtypes, nulls in required fields, negative durations) without being so
brittle that every new column breaks the pipeline.
"""

from __future__ import annotations

import pandera as pa
from pandera.typing import Series

NYC_LAT_RANGE = (39.0, 42.0)
NYC_LON_RANGE = (-75.0, -72.0)


class RawTaxiSchema(pa.DataFrameModel):
    id: Series[str] = pa.Field(nullable=False, unique=True)
    vendor_id: Series[int] = pa.Field(isin=[1, 2])
    pickup_datetime: Series[str] = pa.Field(nullable=False)
    dropoff_datetime: Series[str] = pa.Field(nullable=False)
    passenger_count: Series[int] = pa.Field(ge=0, le=9)
    pickup_longitude: Series[float] = pa.Field(in_range={"min_value": NYC_LON_RANGE[0], "max_value": NYC_LON_RANGE[1]})
    pickup_latitude: Series[float] = pa.Field(in_range={"min_value": NYC_LAT_RANGE[0], "max_value": NYC_LAT_RANGE[1]})
    dropoff_longitude: Series[float] = pa.Field(in_range={"min_value": NYC_LON_RANGE[0], "max_value": NYC_LON_RANGE[1]})
    dropoff_latitude: Series[float] = pa.Field(in_range={"min_value": NYC_LAT_RANGE[0], "max_value": NYC_LAT_RANGE[1]})
    store_and_fwd_flag: Series[str] = pa.Field(isin=["Y", "N"])
    trip_duration: Series[int] = pa.Field(ge=0)

    class Config:
        strict = False  # allow extra columns, we only gate on the ones we care about
        coerce = True


class CleanedTaxiSchema(pa.DataFrameModel):
    id: Series[str] = pa.Field(nullable=False, unique=True)
    trip_duration_minutes: Series[float] = pa.Field(gt=0, lt=300)
    passenger_count: Series[int] = pa.Field(ge=0, le=9)

    class Config:
        strict = False
        coerce = True


def validate(df, schema: type[pa.DataFrameModel], *, sample: int | None = 10_000):
    """Validate a DataFrame against a schema.

    For large Spark-sourced frames, pass a pandas sample (Pandera is pandas-native).
    Returns the validated frame on success; raises SchemaError on failure so the
    pipeline fails loud.
    """
    if sample is not None and len(df) > sample:
        df = df.sample(n=sample, random_state=0)
    return schema.validate(df, lazy=True)
