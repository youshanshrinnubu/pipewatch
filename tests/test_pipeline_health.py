"""Tests for pipewatch.pipeline_health."""
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.pipeline_health import (
    evaluate_pipeline,
    evaluate_all,
    format_health_report,
    _classify,
)


def make_metric(pipeline="pipe", status="ok", records=100, failures=0, duration=1.0):
    return PipelineMetric(
        pipeline=pipeline,
        status=status,
        records_processed=records,
        records_failed=failures,
        duration_seconds=duration,
    )


def test_classify_healthy():
    m = make_metric(records=100, failures=5)
    assert _classify(m) == "healthy"


def test_classify_warning():
    m = make_metric(records=100, failures=25)
    assert _classify(m) == "warning"


def test_classify_critical_high_failure():
    m = make_metric(records=100, failures=60)
    assert _classify(m) == "critical"


def test_classify_critical_error_status():
    m = make_metric(status="error", records=100, failures=0)
    assert _classify(m) == "critical"


def test_evaluate_pipeline_returns_none_for_empty():
    assert evaluate_pipeline("pipe", []) is None


def test_evaluate_pipeline_returns_none_for_unknown():
    m = make_metric(pipeline="other")
    assert evaluate_pipeline("pipe", [m]) is None


def test_evaluate_pipeline_healthy():
    metrics = [make_metric(records=100, failures=2) for _ in range(3)]
    r = evaluate_pipeline("pipe", metrics)
    assert r is not None
    assert r.status == "healthy"
    assert r.total_metrics == 3
    assert r.healthy_count == 3
    assert r.score > 0.8


def test_evaluate_pipeline_critical():
    metrics = [make_metric(records=100, failures=70)]
    r = evaluate_pipeline("pipe", metrics)
    assert r.status == "critical"
    assert r.critical_count == 1


def test_evaluate_pipeline_mixed():
    metrics = [
        make_metric(records=100, failures=5),
        make_metric(records=100, failures=60),
    ]
    r = evaluate_pipeline("pipe", metrics)
    assert r.status == "critical"
    assert r.healthy_count == 1
    assert r.critical_count == 1


def test_evaluate_all_groups_by_pipeline():
    metrics = [
        make_metric(pipeline="a", records=100, failures=2),
        make_metric(pipeline="b", records=100, failures=80),
    ]
    reports = evaluate_all(metrics)
    assert len(reports) == 2
    names = {r.pipeline for r in reports}
    assert names == {"a", "b"}


def test_evaluate_all_empty():
    assert evaluate_all([]) == []


def test_notes_on_high_failure_rate():
    m = make_metric(records=100, failures=40)
    r = evaluate_pipeline("pipe", [m])
    assert any("failure rate" in n.lower() for n in r.notes)


def test_format_health_report_contains_pipeline():
    m = make_metric(pipeline="mypipe")
    r = evaluate_pipeline("mypipe", [m])
    text = format_health_report(r)
    assert "mypipe" in text
    assert "HEALTHY" in text
