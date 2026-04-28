"""Generate synthetic NYC-Taxi-shaped CSVs when Kaggle data isn't available.

Writes ``data/processed/cleaned_train.csv`` and ``cleaned_test.csv``
with the same columns, primary key (``id``), and rough value ranges
that ``etl.load`` and ``scripts/setup_tables.sql`` expect, so the
loader → Postgres → Pushgateway → Prometheus → Grafana path can be
exercised end-to-end without staging real Kaggle CSVs.

This is not the Kaggle dataset. The numeric fields are uniform
random samples in NYC-bounding-box ranges and the durations are drawn
from a gamma-shaped distribution. Reviewers shouldn't read meaning into
the values. The point is wiring: the dashboard's metric queries (rows
loaded, load duration, warehouse live rows) light up without anyone
needing Kaggle credentials.

The schema mirrors ``scripts/benchmark_load.py:make_dataframe``.
Keeping them in lockstep makes it cheap to share fixtures later.

Usage:
    python scripts/generate_synthetic_taxi.py --train-rows 5000 --test-rows 2000
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

TRAIN_DEFAULT = 5_000
TEST_DEFAULT = 2_000


def _build_frame(rows: int, *, start_id: int, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    pickup = pd.Timestamp("2016-01-01") + pd.to_timedelta(
        rng.integers(0, 90 * 24 * 60, size=rows), unit="m"
    )
    duration = rng.integers(60, 3600, size=rows)
    dropoff = pickup + pd.to_timedelta(duration, unit="s")
    return pd.DataFrame(
        {
            "id": [f"syn{i:09d}" for i in range(start_id, start_id + rows)],
            "vendor_id": rng.integers(1, 3, size=rows),
            "pickup_datetime": pickup,
            "dropoff_datetime": dropoff,
            "passenger_count": rng.integers(1, 5, size=rows),
            "pickup_longitude": rng.uniform(-74.05, -73.75, size=rows),
            "pickup_latitude": rng.uniform(40.60, 40.90, size=rows),
            "dropoff_longitude": rng.uniform(-74.05, -73.75, size=rows),
            "dropoff_latitude": rng.uniform(40.60, 40.90, size=rows),
            "store_and_fwd_flag": rng.choice(["Y", "N"], size=rows),
            "trip_duration": duration,
            "trip_duration_minutes": duration / 60.0,
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--train-rows", type=int, default=TRAIN_DEFAULT)
    parser.add_argument("--test-rows", type=int, default=TEST_DEFAULT)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out-dir", type=Path, default=Path("data/processed"))
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    train = _build_frame(args.train_rows, start_id=1, seed=args.seed)
    test = _build_frame(args.test_rows, start_id=args.train_rows + 1, seed=args.seed + 1)

    train_path = args.out_dir / "cleaned_train.csv"
    test_path = args.out_dir / "cleaned_test.csv"
    train.to_csv(train_path, index=False)
    test.to_csv(test_path, index=False)

    print(f"Wrote synthetic cleaned train data: {train_path} ({len(train):,} rows)")
    print(f"Wrote synthetic cleaned test data:  {test_path} ({len(test):,} rows)")
    print(
        "\nThese are synthetic samples, the warehouse rows are not real trips. "
        "Replace with the Kaggle NYC Taxi CSVs (run through scripts/transform.py) "
        "for an actual experiment."
    )


if __name__ == "__main__":
    main()
