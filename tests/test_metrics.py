"""Tests for the Pushgateway-based ETL metrics recorder.

Pins three behaviours that matter:

* Env var unset → no network calls, no errors. The loader must stay usable
  without the observability stack.
* Env var set → ``push_to_gateway`` is called once on ``__exit__`` with
  the right job + instance grouping key and a registry that carries the
  row-count counter we set inside the ``with`` block.
* Pushgateway down → the load does NOT crash; we log and move on.
"""

from __future__ import annotations

from unittest import mock

import pytest
from prometheus_client import CollectorRegistry

from etl.metrics import MetricsRecorder


def test_recorder_is_noop_when_env_unset(monkeypatch):
    monkeypatch.delenv("PROMETHEUS_PUSHGATEWAY", raising=False)
    with (
        mock.patch("etl.metrics.push_to_gateway") as push,
        MetricsRecorder("nyc_taxi_load", "train") as m,
    ):
        m.rows_loaded = 42
    push.assert_not_called()


def test_recorder_pushes_to_gateway_with_grouping_key(monkeypatch):
    monkeypatch.setenv("PROMETHEUS_PUSHGATEWAY", "pushgateway:9091")
    with (
        mock.patch("etl.metrics.push_to_gateway") as push,
        MetricsRecorder("nyc_taxi_load", "train") as m,
    ):
        m.rows_loaded = 100

    push.assert_called_once()
    kwargs = push.call_args.kwargs
    assert kwargs["gateway"] == "pushgateway:9091"
    assert kwargs["job"] == "nyc_taxi_load"
    assert kwargs["grouping_key"] == {"instance": "train"}
    registry = kwargs["registry"]
    assert isinstance(registry, CollectorRegistry)

    # Verify our metrics actually landed on that registry.
    value = registry.get_sample_value(
        "etl_rows_loaded_total", labels={"dataset": "train"}
    )
    assert value == 100


def test_recorder_increments_failures_on_exception(monkeypatch):
    monkeypatch.setenv("PROMETHEUS_PUSHGATEWAY", "pushgateway:9091")
    with (
        mock.patch("etl.metrics.push_to_gateway") as push,
        pytest.raises(RuntimeError),
        MetricsRecorder("nyc_taxi_load", "test"),
    ):
        raise RuntimeError("load blew up")

    push.assert_called_once()
    registry = push.call_args.kwargs["registry"]
    failures = registry.get_sample_value(
        "etl_load_failures_total", labels={"dataset": "test"}
    )
    rows = registry.get_sample_value(
        "etl_rows_loaded_total", labels={"dataset": "test"}
    )
    assert failures == 1
    # Row counter stays at zero on failure; only the failure counter moves.
    assert rows is None or rows == 0


def test_recorder_swallows_pushgateway_errors(monkeypatch, caplog):
    monkeypatch.setenv("PROMETHEUS_PUSHGATEWAY", "pushgateway:9091")
    # The whole point: metrics failures don't crash the load.
    with (
        mock.patch("etl.metrics.push_to_gateway", side_effect=OSError("no route")) as push,
        caplog.at_level("WARNING"),
        MetricsRecorder("nyc_taxi_load", "train") as m,
    ):
        m.rows_loaded = 3

    push.assert_called_once()
    assert any("Failed to push metrics" in rec.message for rec in caplog.records)
