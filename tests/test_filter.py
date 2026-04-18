"""Tests for pipewatch.filter module."""

import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.alerts import Alert
from pipewatch.filter import (
    filter_metrics_by_status,
    filter_metrics_by_pipeline,
    filter_metrics_by_failure_rate,
    filter_alerts_by_severity,
    filter_alerts_by_pipeline,
    compose_filters,
)


def make_metric(name="pipe", status="ok", total=100, failed=0):
    return PipelineMetric(
        pipeline_name=name,
        status=status,
        total_records=total,
        failed_records=failed,
        duration_seconds=1.0,
    )


def make_alert(name="pipe", severity="warning", message="oops"):
    return Alert(pipeline_name=name, severity=severity, message=message)


def test_filter_metrics_by_status_keeps_matching():
    metrics = [make_metric(status="ok"), make_metric(status="error"), make_metric(status="ok")]
    result = filter_metrics_by_status(metrics, ["ok"])
    assert len(result) == 2
    assert all(m.status == "ok" for m in result)


def test_filter_metrics_by_status_empty_result():
    metrics = [make_metric(status="ok")]
    assert filter_metrics_by_status(metrics, ["error"]) == []


def test_filter_metrics_by_pipeline():
    metrics = [make_metric(name="a"), make_metric(name="b"), make_metric(name="a")]
    result = filter_metrics_by_pipeline(metrics, ["a"])
    assert len(result) == 2
    assert all(m.pipeline_name == "a" for m in result)


def test_filter_metrics_by_failure_rate_range():
    metrics = [
        make_metric(total=100, failed=5),   # 0.05
        make_metric(total=100, failed=50),  # 0.50
        make_metric(total=100, failed=90),  # 0.90
    ]
    result = filter_metrics_by_failure_rate(metrics, min_rate=0.0, max_rate=0.1)
    assert len(result) == 1


def test_filter_metrics_by_failure_rate_zero_total():
    metrics = [make_metric(total=0, failed=0)]
    result = filter_metrics_by_failure_rate(metrics, min_rate=0.0, max_rate=0.0)
    assert len(result) == 1


def test_filter_alerts_by_severity():
    alerts = [make_alert(severity="warning"), make_alert(severity="critical"), make_alert(severity="warning")]
    result = filter_alerts_by_severity(alerts, ["critical"])
    assert len(result) == 1
    assert result[0].severity == "critical"


def test_filter_alerts_by_pipeline():
    alerts = [make_alert(name="x"), make_alert(name="y"), make_alert(name="x")]
    result = filter_alerts_by_pipeline(alerts, ["x"])
    assert len(result) == 2


def test_compose_filters():
    metrics = [
        make_metric(name="a", status="ok"),
        make_metric(name="b", status="ok"),
        make_metric(name="a", status="error"),
    ]
    combined = compose_filters([
        lambda ms: filter_metrics_by_pipeline(ms, ["a"]),
        lambda ms: filter_metrics_by_status(ms, ["ok"]),
    ])
    result = combined(metrics)
    assert len(result) == 1
    assert result[0].pipeline_name == "a"
    assert result[0].status == "ok"
