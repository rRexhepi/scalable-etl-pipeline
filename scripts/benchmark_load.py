"""Benchmark the loader: ``pandas.to_sql`` vs. ``copy_upsert``.

Spins up a throwaway Postgres on a Unix socket in ``$PG_BENCH_DIR``
(auto-created under ``/tmp`` if unset), generates a synthetic NYC-Taxi-shaped
DataFrame, and times four paths:

1. ``pandas.to_sql(if_exists='replace')`` — the legacy loader.
2. ``pandas.to_sql(method='multi', chunksize=1000)`` — a common "fast" variant.
3. :func:`utils.db_utils.copy_upsert` on an empty target (initial load).
4. :func:`utils.db_utils.copy_upsert` on a fully-populated target (idempotent rerun).

Usage:
    python scripts/benchmark_load.py --rows 500000

Requires ``postgres``, ``initdb``, and ``pg_ctl`` on ``$PATH`` (Homebrew
``postgresql@14`` works). Does NOT require Docker. Cleans up on exit.
"""

from __future__ import annotations

import argparse
import atexit
import contextlib
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

# Make the project importable when running from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.db_utils import copy_upsert  # noqa: E402

logging.basicConfig(level=logging.WARNING, format="%(message)s")

SCHEMA = """
CREATE TABLE bench_taxi (
    id                      TEXT PRIMARY KEY,
    vendor_id               INTEGER,
    pickup_datetime         TIMESTAMP NOT NULL,
    dropoff_datetime        TIMESTAMP,
    passenger_count         INTEGER,
    pickup_longitude        DOUBLE PRECISION,
    pickup_latitude         DOUBLE PRECISION,
    dropoff_longitude       DOUBLE PRECISION,
    dropoff_latitude        DOUBLE PRECISION,
    store_and_fwd_flag      TEXT,
    trip_duration           INTEGER,
    trip_duration_minutes   DOUBLE PRECISION
);
"""


def make_dataframe(rows: int, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    pickup = pd.Timestamp("2016-01-01") + pd.to_timedelta(
        rng.integers(0, 90 * 24 * 60, size=rows), unit="m"
    )
    duration = rng.integers(60, 3600, size=rows)
    dropoff = pickup + pd.to_timedelta(duration, unit="s")
    return pd.DataFrame(
        {
            "id": [f"id{i:09d}" for i in range(rows)],
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


class ThrowawayPostgres:
    """Minimal manager for a single-user Postgres instance on a Unix socket."""

    def __init__(self, basedir: Path):
        self.basedir = basedir
        self.datadir = basedir / "data"
        self.socket = basedir / "sock"
        self.logfile = basedir / "pg.log"
        self.port = 55432
        self.dbname = "bench"
        self._proc_started = False

    def start(self) -> None:
        self.datadir.mkdir(parents=True, exist_ok=True)
        self.socket.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["initdb", "-D", str(self.datadir), "-U", os.environ["USER"], "-A", "trust"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            [
                "pg_ctl",
                "-D", str(self.datadir),
                "-l", str(self.logfile),
                "-o", f"-p {self.port} -k {self.socket} -c listen_addresses='' -c fsync=off "
                      f"-c synchronous_commit=off -c full_page_writes=off",
                "start",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        self._proc_started = True
        for _ in range(50):
            r = subprocess.run(
                ["pg_isready", "-h", str(self.socket), "-p", str(self.port)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if r.returncode == 0:
                break
            time.sleep(0.1)
        else:
            raise RuntimeError("Postgres never became ready")
        subprocess.run(
            ["createdb", "-h", str(self.socket), "-p", str(self.port), self.dbname],
            check=True,
        )

    def stop(self) -> None:
        if not self._proc_started:
            return
        with contextlib.suppress(Exception):
            subprocess.run(
                ["pg_ctl", "-D", str(self.datadir), "-m", "fast", "stop"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
            )
        self._proc_started = False

    def engine_url(self) -> str:
        user = os.environ["USER"]
        return (
            f"postgresql://{user}@/{self.dbname}"
            f"?host={self.socket}&port={self.port}"
        )


def _time(label: str, fn) -> float:
    start = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - start
    rows = result if isinstance(result, int) else None
    suffix = f" ({rows:,} rows)" if rows is not None else ""
    print(f"  {label:<42} {elapsed:7.2f}s{suffix}")
    return elapsed


def run(rows: int) -> None:
    df = make_dataframe(rows)
    print(f"Benchmarking with {rows:,} rows ({df.memory_usage(deep=True).sum() / 1e6:.1f} MB)\n")

    basedir = Path(tempfile.mkdtemp(prefix="pg-bench-"))
    pg = ThrowawayPostgres(basedir)

    def _cleanup():
        pg.stop()
        shutil.rmtree(basedir, ignore_errors=True)

    atexit.register(_cleanup)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: (_cleanup(), sys.exit(1)))

    pg.start()
    engine = create_engine(pg.engine_url(), future=True)

    def _reset_target(create_pk: bool) -> None:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS bench_taxi"))
            if create_pk:
                conn.execute(text(SCHEMA))

    def _row_count() -> int:
        with engine.begin() as conn:
            return conn.execute(text("SELECT COUNT(*) FROM bench_taxi")).scalar_one()

    # ---- 1. to_sql(if_exists='replace')
    _reset_target(create_pk=False)
    t_replace = _time(
        "pandas.to_sql(if_exists='replace')",
        lambda: (df.to_sql("bench_taxi", engine, if_exists="replace", index=False), _row_count())[1],
    )

    # ---- 2. to_sql method='multi'
    _reset_target(create_pk=False)
    t_multi = _time(
        "pandas.to_sql(method='multi', chunksize=1000)",
        lambda: (
            df.to_sql(
                "bench_taxi", engine, if_exists="replace",
                index=False, method="multi", chunksize=1000,
            ),
            _row_count(),
        )[1],
    )

    # ---- 3. COPY + upsert, empty target
    _reset_target(create_pk=True)
    t_copy_empty = _time(
        "copy_upsert (empty target)",
        lambda: copy_upsert(
            engine, df,
            target_table="bench_taxi",
            conflict_columns=["id"],
        ),
    )

    # ---- 4. COPY + upsert, full target (idempotent rerun)
    _time(
        "copy_upsert (rerun vs full target)",
        lambda: copy_upsert(
            engine, df,
            target_table="bench_taxi",
            conflict_columns=["id"],
        ),
    )

    assert _row_count() == rows, "row count diverged after idempotent rerun"

    print("\nSpeedup (initial load):")
    print(f"  COPY vs to_sql(replace):  {t_replace / t_copy_empty:6.1f}x")
    print(f"  COPY vs to_sql(multi):    {t_multi / t_copy_empty:6.1f}x")

    _cleanup()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=500_000, help="Rows to load.")
    args = parser.parse_args()
    run(args.rows)


if __name__ == "__main__":
    main()
