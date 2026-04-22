"""Load cleaned NYC Taxi CSVs into Postgres.

Replaces an earlier ``pandas.to_sql(if_exists='replace')`` path which was
destructive (dropped and recreated the table on every run) and slow
(row-by-row INSERTs under the hood). This version:

* Uses Postgres ``COPY`` through a session-temp staging table — see
  :func:`utils.db_utils.copy_upsert` for the mechanics.
* Deduplicates on a real primary key (``id`` from the Kaggle dataset),
  so reruns are idempotent and row-level corrections propagate.
* Applies an incremental watermark on ``pickup_datetime`` so we only
  process rows newer than what's already in the warehouse. Full
  reprocessing is available via ``--no-watermark``.
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass

import pandas as pd
import yaml

from etl.metrics import MetricsRecorder
from utils.db_utils import copy_upsert, get_db_engine, get_watermark
from utils.logger import setup_logger

METRICS_JOB = "nyc_taxi_load"


@dataclass(frozen=True)
class DatasetSpec:
    """Per-dataset load configuration.

    ``conflict_column`` is the target's primary key. ``watermark_column`` is
    the monotonically-increasing column used to filter inputs on reruns; set
    to ``None`` to disable incremental loading for a dataset.
    """

    key: str
    source_file: str
    target_table: str
    conflict_column: str = "id"
    watermark_column: str | None = "pickup_datetime"


DATASETS = (
    DatasetSpec(
        key="train",
        source_file="cleaned_train.csv",
        target_table="nyc_taxi_trip_duration_train",
    ),
    DatasetSpec(
        key="test",
        source_file="cleaned_test.csv",
        target_table="nyc_taxi_trip_duration_test",
    ),
)


_DATETIME_COLUMNS = ("pickup_datetime", "dropoff_datetime")


def _read_cleaned_csv(path: str) -> pd.DataFrame:
    """Read a Spark-produced cleaned CSV, coercing timestamp columns."""
    df = pd.read_csv(path)
    for col in _DATETIME_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def _apply_watermark(
    df: pd.DataFrame,
    engine,
    spec: DatasetSpec,
    logger: logging.Logger,
) -> pd.DataFrame:
    if spec.watermark_column is None or spec.watermark_column not in df.columns:
        return df
    watermark = get_watermark(engine, spec.target_table, spec.watermark_column)
    if watermark is None:
        logger.info("No existing watermark for %s; loading full file.", spec.target_table)
        return df
    before = len(df)
    df = df[df[spec.watermark_column] > pd.Timestamp(watermark)]
    logger.info(
        "Watermark %s on %s filtered %s -> %s rows.",
        watermark,
        spec.target_table,
        before,
        len(df),
    )
    return df


def load_data(config_path: str = "config/config.yaml", *, use_watermark: bool = True) -> dict[str, int]:
    """Load cleaned CSVs into Postgres and return per-dataset row counts."""
    logger = setup_logger()
    logger.info("Starting data loading process.")

    if not os.path.exists(config_path):
        logger.error("Configuration file not found at %s", config_path)
        raise FileNotFoundError(f"Configuration file not found at {config_path}")

    with open(config_path) as file:
        config = yaml.safe_load(file)

    processed_data_dir = config.get("paths", {}).get("processed_data")
    if not processed_data_dir:
        logger.error("Processed data directory not specified in the configuration.")
        raise KeyError("Processed data directory not specified in the configuration.")

    engine = get_db_engine(config_path=config_path)
    logger.info("Database engine created successfully.")

    loaded: dict[str, int] = {}
    for spec in DATASETS:
        file_path = os.path.join(processed_data_dir, spec.source_file)
        if not os.path.exists(file_path):
            logger.warning("Processed %s file not found at %s. Skipping.", spec.key, file_path)
            loaded[spec.key] = 0
            continue

        logger.info("Loading %s data from %s into %s.", spec.key, file_path, spec.target_table)
        df = _read_cleaned_csv(file_path)
        if use_watermark:
            df = _apply_watermark(df, engine, spec, logger)

        with MetricsRecorder(METRICS_JOB, spec.key) as metrics:
            rows = copy_upsert(
                engine,
                df,
                target_table=spec.target_table,
                conflict_columns=[spec.conflict_column],
            )
            metrics.rows_loaded = rows
        loaded[spec.key] = rows
        logger.info("Upserted %s rows into %s.", rows, spec.target_table)

    logger.info("Data loading process completed successfully.")
    return loaded


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config", default="config/config.yaml", help="Path to config YAML."
    )
    parser.add_argument(
        "--no-watermark",
        action="store_true",
        help="Ignore the watermark and reprocess every row (useful for backfills).",
    )
    args = parser.parse_args()

    try:
        load_data(config_path=args.config, use_watermark=not args.no_watermark)
    except Exception as e:
        logging.error("Data loading failed: %s", e)
        raise SystemExit(1) from e


if __name__ == "__main__":
    main()
