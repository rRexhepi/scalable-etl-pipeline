"""Prometheus metrics for one-shot ETL jobs.

The loader is a batch process, not a long-running server, so we push
metrics to Prometheus's **Pushgateway** instead of exposing a `/metrics`
endpoint. Prometheus scrapes the Pushgateway on its normal interval;
Grafana graphs the result.

Design choices worth knowing:

* **No-op when unset.** `PROMETHEUS_PUSHGATEWAY` controls the destination.
  When the env var is missing (CI, unit tests, local dev without the
  observability stack), :class:`MetricsRecorder` is a silent drop-in, so
  the load path never gains a hard dep on Prometheus being up.
* **Explicit job + instance labels.** Per Prometheus guidance, one
  ``job`` per pipeline and a distinguishing ``instance`` (here, the
  dataset name) so series from the ``train`` and ``test`` loads stay
  separate on the same Pushgateway.
* **Push at end only.** Duration and row counts are meaningful only
  once a dataset is fully loaded; pushing mid-run would just add noise.

Wire it from the loader as::

    with MetricsRecorder("nyc_taxi_load", "train") as m:
        rows = copy_upsert(...)
        m.rows_loaded = rows
"""

from __future__ import annotations

import logging
import os
import time

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    push_to_gateway,
)

logger = logging.getLogger(__name__)

PUSHGATEWAY_ENV = "PROMETHEUS_PUSHGATEWAY"


class MetricsRecorder:
    """Context manager that records per-dataset load metrics and pushes on exit.

    Each instance owns its own :class:`CollectorRegistry` so concurrent
    loads (train + test) don't clobber each other's series labels.
    """

    def __init__(self, job: str, instance: str, *, gateway: str | None = None):
        self.job = job
        self.instance = instance
        self.gateway = gateway or os.getenv(PUSHGATEWAY_ENV)
        self.rows_loaded: int = 0

        self._registry = CollectorRegistry()
        self._rows_counter = Counter(
            "etl_rows_loaded_total",
            "Rows upserted into the warehouse.",
            labelnames=("dataset",),
            registry=self._registry,
        )
        self._duration_gauge = Gauge(
            "etl_load_duration_seconds",
            "Wall-clock duration of the load step.",
            labelnames=("dataset",),
            registry=self._registry,
        )
        self._success_gauge = Gauge(
            "etl_load_last_success_timestamp_seconds",
            "Unix timestamp of the last successful load.",
            labelnames=("dataset",),
            registry=self._registry,
        )
        self._failure_counter = Counter(
            "etl_load_failures_total",
            "Failed load attempts.",
            labelnames=("dataset",),
            registry=self._registry,
        )

    def __enter__(self) -> MetricsRecorder:
        self._start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        duration = time.perf_counter() - self._start
        self._duration_gauge.labels(dataset=self.instance).set(duration)
        if exc_type is None:
            self._rows_counter.labels(dataset=self.instance).inc(self.rows_loaded)
            self._success_gauge.labels(dataset=self.instance).set(time.time())
        else:
            self._failure_counter.labels(dataset=self.instance).inc()
        self._push()

    def _push(self) -> None:
        if not self.gateway:
            logger.debug(
                "%s unset; skipping Pushgateway push for job=%s instance=%s",
                PUSHGATEWAY_ENV,
                self.job,
                self.instance,
            )
            return
        try:
            push_to_gateway(
                gateway=self.gateway,
                job=self.job,
                registry=self._registry,
                grouping_key={"instance": self.instance},
            )
            logger.info(
                "Pushed metrics to %s (job=%s instance=%s rows=%s)",
                self.gateway,
                self.job,
                self.instance,
                self.rows_loaded,
            )
        except Exception as e:
            # Metrics failures must never fail the load itself.
            logger.warning("Failed to push metrics to %s: %s", self.gateway, e)
