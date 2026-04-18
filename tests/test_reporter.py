"""Tests for pipewatch.reporter."""

import pytest
from unittest.mock import MagicMock
from pipewatch.metrics import PipelineMetric
from pipewatch.reporter import build_report, build_report_from_store, _header


def make_metric(name="pipe_a", status="ok", total=100, failed=2):
    return PipelineMetric(
        pipeline_name=name,
        status=status,
        records_processed=total,
        records_failed=failed,
        duration_seconds=1.5,
    )


def test_header_contains_title():
    h = _header("My Title")
    assert "My Title" in h
    assert "=" in h


def test_build_report_contains_pipeline_name():
    m = make_metric(name="sales_etl")
    report = build_report([m])
    assert "sales_etl" in report


def test_build_report_contains_header_title():
    report = build_report([make_metric()], title="Custom Report Title")
    assert "Custom Report Title" in report


def test_build_report_empty_metrics():
    report = build_report([])
    assert "No pipeline metrics available" in report


def test_build_report_shows_healthy_count():
    metrics = [make_metric("a", "ok", 100, 1), make_metric("b", "error", 100, 80)]
    report = build_report(metrics)
    assert "Pipelines healthy:" in report


def test_build_report_timestamp_included_by_default():
    report = build_report([make_metric()])
    assert "Generated:" in report


def test_build_report_no_timestamp():
    report = build_report([make_metric()], include_timestamp=False)
    assert "Generated:" not in report


def test_build_report_multiple_pipelines():
    metrics = [make_metric("alpha"), make_metric("beta")]
    report = build_report(metrics)
    assert "alpha" in report
    assert "beta" in report


def test_build_report_from_store_uses_latest_metric():
    store = MagicMock()
    m = make_metric("pipe_x")
    store.list_pipelines.return_value = ["pipe_x"]
    store.load.return_value = [m]
    report = build_report_from_store(store)
    assert "pipe_x" in report


def test_build_report_from_store_empty_store():
    store = MagicMock()
    store.list_pipelines.return_value = []
    report = build_report_from_store(store)
    assert "No pipeline metrics available" in report
