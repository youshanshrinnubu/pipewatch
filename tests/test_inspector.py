"""Tests for pipewatch.inspector."""
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.inspector import (
    inspect_pipeline,
    inspect_all,
    format_inspection,
    InspectionReport,
)


def make_metric(pipeline="pipe", records=100, failures=0, status="ok", ts=1_000_000.0):
    return PipelineMetric(
        pipeline_name=pipeline,
        records_processed=records,
        records_failed=failures,
        status=status,
        timestamp=ts,
    )


def test_inspect_returns_none_for_unknown_pipeline():
    metrics = [make_metric("pipe_a")]
    assert inspect_pipeline("pipe_b", metrics) is None


def test_inspect_single_healthy_metric():
    m = make_metric(records=100, failures=0, status="ok")
    r = inspect_pipeline("pipe", [m])
    assert r is not None
    assert r.total_runs == 1
    assert r.avg_failure_rate == 0.0
    assert r.verdict == "healthy"
    assert r.ok_count == 1
    assert r.error_count == 0


def test_inspect_degraded_on_moderate_failure_rate():
    metrics = [make_metric(records=100, failures=25) for _ in range(4)]
    r = inspect_pipeline("pipe", metrics)
    assert r.verdict == "degraded"
    assert abs(r.avg_failure_rate - 0.25) < 1e-6


def test_inspect_critical_on_high_failure_rate():
    metrics = [make_metric(records=100, failures=60)]
    r = inspect_pipeline("pipe", metrics)
    assert r.verdict == "critical"


def test_inspect_critical_on_error_status():
    metrics = [make_metric(records=100, failures=0, status="error")]
    r = inspect_pipeline("pipe", metrics)
    assert r.verdict == "critical"
    assert r.error_count == 1


def test_inspect_max_min_failure_rate():
    metrics = [
        make_metric(records=100, failures=10),
        make_metric(records=100, failures=50),
        make_metric(records=100, failures=0),
    ]
    r = inspect_pipeline("pipe", metrics)
    assert r.max_failure_rate == pytest.approx(0.5)
    assert r.min_failure_rate == pytest.approx(0.0)


def test_inspect_last_status():
    metrics = [
        make_metric(status="ok", ts=1.0),
        make_metric(status="error", ts=2.0),
    ]
    r = inspect_pipeline("pipe", metrics)
    assert r.last_status == "error"


def test_inspect_all_returns_one_per_pipeline():
    metrics = [
        make_metric("alpha"),
        make_metric("beta"),
        make_metric("alpha"),
    ]
    reports = inspect_all(metrics)
    names = [r.pipeline for r in reports]
    assert sorted(names) == ["alpha", "beta"]


def test_to_dict_has_expected_keys():
    r = inspect_pipeline("pipe", [make_metric()])
    d = r.to_dict()
    for key in ("pipeline", "verdict", "total_runs", "avg_failure_rate", "last_status"):
        assert key in d


def test_format_inspection_contains_pipeline_name():
    r = inspect_pipeline("my_pipe", [make_metric("my_pipe")])
    text = format_inspection(r)
    assert "my_pipe" in text
    assert "HEALTHY" in text
