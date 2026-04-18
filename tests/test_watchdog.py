"""Tests for pipewatch.watchdog."""

from datetime import datetime, timezone, timedelta

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.watchdog import check_staleness, StalenessResult


def make_metric(pipeline: str, age_seconds: float, status: str = "ok") -> PipelineMetric:
    ts = datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
    return PipelineMetric(pipeline, status, 100, 1, ts)


def test_fresh_pipeline_is_ok():
    metrics = [make_metric("pipe_a", 10)]
    results = check_staleness(metrics, warning_seconds=300, critical_seconds=900)
    assert len(results) == 1
    r = results[0]
    assert r.severity == "ok"
    assert not r.is_stale


def test_warning_threshold():
    metrics = [make_metric("pipe_b", 400)]
    results = check_staleness(metrics, warning_seconds=300, critical_seconds=900)
    assert results[0].severity == "warning"
    assert results[0].is_stale


def test_critical_threshold():
    metrics = [make_metric("pipe_c", 1000)]
    results = check_staleness(metrics, warning_seconds=300, critical_seconds=900)
    assert results[0].severity == "critical"
    assert results[0].is_stale


def test_uses_most_recent_metric_per_pipeline():
    old = make_metric("pipe_d", 1000)
    recent = make_metric("pipe_d", 30)
    results = check_staleness([old, recent], warning_seconds=300, critical_seconds=900)
    assert len(results) == 1
    assert results[0].severity == "ok"


def test_multiple_pipelines_returned():
    metrics = [make_metric("a", 10), make_metric("b", 500), make_metric("c", 1200)]
    results = check_staleness(metrics)
    assert len(results) == 3
    severities = {r.pipeline: r.severity for r in results}
    assert severities["a"] == "ok"
    assert severities["b"] == "warning"
    assert severities["c"] == "critical"


def test_to_dict_keys():
    metrics = [make_metric("pipe_e", 50)]
    r = check_staleness(metrics)[0]
    d = r.to_dict()
    assert set(d.keys()) == {"pipeline", "last_seen", "age_seconds", "is_stale", "severity"}


def test_empty_metrics_returns_empty():
    assert check_staleness([]) == []


def test_age_seconds_approximate():
    metrics = [make_metric("pipe_f", 120)]
    r = check_staleness(metrics)[0]
    assert 115 <= r.age_seconds <= 125
