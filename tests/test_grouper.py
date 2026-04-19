"""Tests for pipewatch.grouper."""
from datetime import datetime, timezone
from pipewatch.metrics import PipelineMetric
from pipewatch.alerts import Alert
from pipewatch.grouper import (
    group_metrics_by_status,
    group_metrics_by_pipeline,
    group_alerts_by_severity,
    format_groups,
)


def make_metric(name="pipe", total=100, failed=5, status="ok"):
    return PipelineMetric(name, total, failed, status, datetime.now(timezone.utc))


def make_alert(pipeline="pipe", severity="warn", message="alert"):
    return Alert(pipeline, severity, message)


def test_group_metrics_by_status_single_group():
    metrics = [make_metric(status="ok"), make_metric(name="b", status="ok")]
    groups = group_metrics_by_status(metrics)
    assert "ok" in groups
    assert len(groups["ok"].metrics) == 2


def test_group_metrics_by_status_multiple_groups():
    metrics = [make_metric(status="ok"), make_metric(name="b", status="error")]
    groups = group_metrics_by_status(metrics)
    assert set(groups.keys()) == {"ok", "error"}


def test_group_metrics_by_status_empty():
    groups = group_metrics_by_status([])
    assert groups == {}


def test_group_metrics_by_pipeline():
    metrics = [
        make_metric(name="sales"),
        make_metric(name="sales"),
        make_metric(name="billing"),
    ]
    groups = group_metrics_by_pipeline(metrics)
    assert "sales" in groups
    assert len(groups["sales"].metrics) == 2
    assert "billing" in groups


def test_group_alerts_by_severity():
    alerts = [
        make_alert(severity="warn"),
        make_alert(pipeline="b", severity="critical"),
        make_alert(pipeline="c", severity="warn"),
    ]
    groups = group_alerts_by_severity(alerts)
    assert len(groups["warn"].alerts) == 2
    assert len(groups["critical"].alerts) == 1


def test_group_alerts_by_severity_empty():
    assert group_alerts_by_severity([]) == {}


def test_metric_group_to_dict():
    metrics = [make_metric(name="sales", status="ok")]
    groups = group_metrics_by_status(metrics)
    d = groups["ok"].to_dict()
    assert d["key"] == "ok"
    assert d["count"] == 1
    assert "sales" in d["pipelines"]


def test_format_groups_nonempty():
    metrics = [make_metric(status="ok"), make_metric(name="b", status="error")]
    groups = group_metrics_by_status(metrics)
    out = format_groups(groups)
    assert "ok" in out
    assert "error" in out


def test_format_groups_empty():
    out = format_groups({})
    assert out == "No groups."
