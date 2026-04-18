"""Tests for AlertManager and Alert."""
import pytest
from datetime import datetime
from pipewatch.alerts import Alert, AlertManager
from pipewatch.metrics import PipelineMetric


def make_metric(name="pipe", status="ok", total=100, failed=0):
    return PipelineMetric(
        pipeline_name=name,
        status=status,
        total_records=total,
        failed_records=failed,
        timestamp=datetime.utcnow(),
    )


def test_no_alert_when_healthy():
    mgr = AlertManager(failure_rate_threshold=0.1)
    metric = make_metric(total=100, failed=5, status="ok")
    assert mgr.evaluate(metric) is None


def test_alert_fired_on_high_failure_rate():
    mgr = AlertManager(failure_rate_threshold=0.1)
    metric = make_metric(total=100, failed=20, status="ok")
    alert = mgr.evaluate(metric)
    assert alert is not None
    assert alert.pipeline_name == "pipe"
    assert alert.severity == "warning"


def test_critical_alert_on_very_high_failure_rate():
    mgr = AlertManager(failure_rate_threshold=0.1)
    metric = make_metric(total=100, failed=30, status="ok")
    alert = mgr.evaluate(metric)
    assert alert is not None
    assert alert.severity == "critical"


def test_critical_alert_on_error_status():
    mgr = AlertManager(failure_rate_threshold=0.1)
    metric = make_metric(total=100, failed=15, status="error")
    alert = mgr.evaluate(metric)
    assert alert is not None
    assert alert.severity == "critical"


def test_handler_called_on_alert():
    received = []
    mgr = AlertManager(failure_rate_threshold=0.1)
    mgr.register_handler(received.append)
    metric = make_metric(total=100, failed=20, status="ok")
    mgr.evaluate(metric)
    assert len(received) == 1
    assert received[0].pipeline_name == "pipe"


def test_alert_to_dict_keys():
    alert = Alert(pipeline_name="p", reason="r", severity="warning")
    d = alert.to_dict()
    assert set(d.keys()) == {"pipeline_name", "reason", "severity", "triggered_at", "metric_snapshot"}


def test_no_handler_no_error():
    mgr = AlertManager(failure_rate_threshold=0.05)
    metric = make_metric(total=100, failed=10, status="ok")
    alert = mgr.evaluate(metric)
    assert alert is not None
