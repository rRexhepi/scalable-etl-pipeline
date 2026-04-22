from __future__ import annotations

import io
import logging
import os
from collections.abc import Iterable

import pandas as pd
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def get_db_engine(config_path: str = "config/config.yaml") -> Engine:
    """
    Creates and returns a SQLAlchemy engine for connecting to the PostgreSQL database.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at {config_path}")

    with open(config_path) as file:
        config = yaml.safe_load(file)

    db_config = config.get("database")
    if not db_config:
        raise KeyError("Database configuration not found in the config file.")

    required_keys = ["host", "port", "user", "password", "dbname"]
    for key in required_keys:
        if key not in db_config:
            raise KeyError(f"Database configuration missing '{key}' key.")

    engine_url = (
        f"postgresql://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    )
    try:
        engine = create_engine(engine_url)
    except Exception as e:
        raise ConnectionError(f"Failed to create engine: {e}") from e

    return engine


def get_watermark(engine: Engine, table: str, column: str):
    """Return MAX(column) from table, or None if the table is empty / absent."""
    with engine.begin() as conn:
        return conn.execute(text(f'SELECT MAX("{column}") FROM "{table}"')).scalar()


def copy_upsert(
    engine: Engine,
    df: pd.DataFrame,
    *,
    target_table: str,
    conflict_columns: Iterable[str],
    update_columns: Iterable[str] | None = None,
) -> int:
    """Bulk-load `df` into `target_table` via Postgres COPY + ON CONFLICT upsert.

    The flow is:
      1. CREATE TEMP TABLE staging (LIKE target INCLUDING DEFAULTS) ON COMMIT DROP
      2. COPY staging FROM STDIN (CSV, no header)
      3. INSERT INTO target SELECT ... FROM staging ON CONFLICT (pk) DO UPDATE SET ...

    The whole thing runs in one transaction: on failure, nothing is visible
    to other readers; on retry, the upsert semantics make it idempotent.

    Args:
        engine: SQLAlchemy engine bound to a psycopg2 driver.
        df: DataFrame whose columns are a subset of `target_table`'s columns.
        target_table: Name of the destination table (must already exist).
        conflict_columns: PK / unique-constraint columns used by ON CONFLICT.
        update_columns: Columns to overwrite on conflict. Defaults to every
            non-conflict column in `df` (so corrections in upstream data
            propagate). Pass an empty iterable to get ON CONFLICT DO NOTHING.

    Returns:
        Rows COPYed into staging (pre-dedup). The number of rows actually
        inserted into target can be lower if duplicates within the batch
        collapse on the conflict key.
    """
    if df.empty:
        logger.info("copy_upsert: empty DataFrame, skipping %s", target_table)
        return 0

    columns = list(df.columns)
    conflict = list(conflict_columns)
    if not conflict:
        raise ValueError("copy_upsert requires at least one conflict column")

    if update_columns is None:
        update_cols = [c for c in columns if c not in conflict]
    else:
        update_cols = list(update_columns)

    cols_sql = ", ".join(f'"{c}"' for c in columns)
    conflict_sql = ", ".join(f'"{c}"' for c in conflict)

    staging = f"_staging_{target_table}"

    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute(
            f'CREATE TEMP TABLE "{staging}" '
            f'(LIKE "{target_table}" INCLUDING DEFAULTS) '
            f"ON COMMIT DROP"
        )

        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False, na_rep="")
        buf.seek(0)
        cur.copy_expert(
            f'COPY "{staging}" ({cols_sql}) '
            f"FROM STDIN WITH (FORMAT csv, NULL '')",
            buf,
        )
        copied = cur.rowcount

        if update_cols:
            update_sql = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
            on_conflict = f"ON CONFLICT ({conflict_sql}) DO UPDATE SET {update_sql}"
        else:
            on_conflict = f"ON CONFLICT ({conflict_sql}) DO NOTHING"

        cur.execute(
            f'INSERT INTO "{target_table}" ({cols_sql}) '
            f'SELECT {cols_sql} FROM "{staging}" '
            f"{on_conflict}"
        )
        cur.close()
        raw.commit()
        logger.info(
            "copy_upsert: %s rows staged into %s (conflict=%s)",
            copied,
            target_table,
            conflict,
        )
        return copied
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()
