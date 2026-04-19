"""Tests for pipewatch.splitter."""
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.alerts import Alert
from pipewatch.splitter import (
    split_metrics_by_status,
    split_metrics_by_pipeline,
    split_alerts_by_severity,
    split_alerts_by_pipeline,
    format_split,
)


def make_metric(name="pipe", status="ok", failures=0, total=100):
    return PipelineMetric(
        pipeline_name=name, status=status,
        records_processed=total, records_failed=failures,
        duration_seconds=1.0,
    )


def make_alert(pipeline="pipe", severity="warning", message="msg"):
    return Alert(pipeline_name=pipeline, severity=severity, message=message)


def test_split_metrics_by_status_single_group():
    metrics = [make_metric(status="ok"), make_metric(status="ok")]
    result = split_metrics_by_status(metrics)
    assert set(result.keys()) == {"ok"}
    assert len(result["ok"].items) == 2


def test_split_metrics_by_status_multiple_groups():
    metrics = [make_metric(status="ok"), make_metric(status="error"), make_metric(status="ok")]
    result = split_metrics_by_status(metrics)
    assert len(result["ok"].items) == 2
    assert len(result["error"].items) == 1


def test_split_metrics_by_status_empty():
    assert split_metrics_by_status([]) == {}


def test_split_metrics_by_pipeline():
    metrics = [make_metric(name="a"), make_metric(name="b"), make_metric(name="a")]
    result = split_metrics_by_pipeline(metrics)
    assert len(result["a"].items) == 2
    assert len(result["b"].items) == 1


def test_split_alerts_by_severity():
    alerts = [make_alert(severity="warning"), make_alert(severity="critical"), make_alert(severity="warning")]
    result = split_alerts_by_severity(alerts)
    assert len(result["warning"].items) == 2
    assert len(result["critical"].items) == 1


def test_split_alerts_by_pipeline():
    alerts = [make_alert(pipeline="x"), make_alert(pipeline="y"), make_alert(pipeline="x")]
    result = split_alerts_by_pipeline(alerts)
    assert len(result["x"].items) == 2
    assert len(result["y"].items) == 1


def test_split_result_to_dict():
    metrics = [make_metric()]
    result = split_metrics_by_status(metrics)
    d = result["ok"].to_dict()
    assert d["key"] == "ok"
    assert d["count"] == 1


def test_format_split_empty():
    assert "No items" in format_split({})


def test_format_split_shows_keys():
    metrics = [make_metric(status="ok"), make_metric(status="error")]
    result = split_metrics_by_status(metrics)
    text = format_split(result)
    assert "ok" in text
    assert "error" in text
