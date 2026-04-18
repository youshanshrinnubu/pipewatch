import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.anomaly import AnomalyResult, detect_anomalies


def make_metric(pipeline="pipe", total=1000, failed=0, status="ok"):
    return PipelineMetric(pipeline, total_records=total, failed_records=failed, status=status)


def test_no_anomaly_when_healthy():
    m = make_metric(failed=0)
    results = detect_anomalies([m])
    assert results == []


def test_warning_on_moderate_failure_rate():
    m = make_metric(failed=150)  # 15%
    results = detect_anomalies([m])
    assert len(results) == 1
    assert results[0].severity == "warning"
    assert results[0].pipeline == "pipe"


def test_critical_on_high_failure_rate():
    m = make_metric(failed=400)  # 40%
    results = detect_anomalies([m])
    assert any(r.severity == "critical" for r in results)


def test_error_status_is_critical():
    m = make_metric(failed=0, status="error")
    results = detect_anomalies([m])
    assert len(results) == 1
    assert results[0].severity == "critical"
    assert "error" in results[0].reason


def test_spike_detection():
    m = make_metric(pipeline="p", total=1000, failed=300)  # 30%
    history = [
        make_metric(pipeline="p", total=1000, failed=10),
        make_metric(pipeline="p", total=1000, failed=20),
    ]  # avg ~1.5%
    results = detect_anomalies([m], history=history)
    reasons = [r.reason for r in results]
    assert any("spike" in r for r in reasons)


def test_no_spike_without_history():
    m = make_metric(failed=50)  # 5% — below warning
    results = detect_anomalies([m])
    assert results == []


def test_to_dict_contains_fields():
    m = make_metric(failed=200)
    r = AnomalyResult("pipe", m, "test reason", "warning")
    d = r.to_dict()
    assert d["pipeline"] == "pipe"
    assert d["severity"] == "warning"
    assert d["reason"] == "test reason"
    assert "failure_rate" in d
    assert "status" in d


def test_multiple_pipelines():
    metrics = [
        make_metric("a", failed=0),
        make_metric("b", failed=500),
        make_metric("c", failed=0, status="error"),
    ]
    results = detect_anomalies(metrics)
    pipelines = {r.pipeline for r in results}
    assert "b" in pipelines
    assert "c" in pipelines
    assert "a" not in pipelines


def test_custom_thresholds():
    m = make_metric(failed=50)  # 5%
    results = detect_anomalies([m], failure_rate_warning=0.03, failure_rate_critical=0.08)
    assert len(results) == 1
    assert results[0].severity == "warning"
