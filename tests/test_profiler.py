"""Tests for pipewatch.profiler."""
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.profiler import profile_pipeline, profile_all, format_profiled, _grade


def make_metric(pipeline="sales", records=1000, duration_s=1.0, status="ok", failures=0):
    return PipelineMetric(
        pipeline=pipeline,
        records_processed=records,
        records_failed=failures,
        status=status,
        duration_s=duration_s,
    )


def test_profile_pipeline_returns_none_for_empty():
    assert profile_pipeline([]) is None


def test_profile_pipeline_returns_none_when_no_duration():
    m = make_metric(duration_s=None)
    assert profile_pipeline([m]) is None


def test_profile_pipeline_single_metric():
    m = make_metric(records=500, duration_s=0.5)
    r = profile_pipeline([m])
    assert r is not None
    assert r.pipeline == "sales"
    assert r.sample_count == 1
    assert r.throughput_per_s == pytest.approx(1000.0)
    assert r.avg_duration_s == pytest.approx(0.5)
    assert r.min_duration_s == pytest.approx(0.5)
    assert r.max_duration_s == pytest.approx(0.5)


def test_profile_pipeline_multiple_metrics():
    metrics = [
        make_metric(records=200, duration_s=2.0),
        make_metric(records=400, duration_s=2.0),
    ]
    r = profile_pipeline(metrics)
    assert r.sample_count == 2
    assert r.avg_records == pytest.approx(300.0)
    assert r.throughput_per_s == pytest.approx(150.0)


def test_grade_fast():
    assert _grade(1500) == "fast"


def test_grade_moderate():
    assert _grade(500) == "moderate"


def test_grade_slow():
    assert _grade(50) == "slow"


def test_profile_all_groups_by_pipeline():
    metrics = [
        make_metric(pipeline="a", records=2000, duration_s=1.0),
        make_metric(pipeline="b", records=10, duration_s=1.0),
        make_metric(pipeline="a", records=2000, duration_s=1.0),
    ]
    results = profile_all(metrics)
    names = [r.pipeline for r in results]
    assert "a" in names
    assert "b" in names
    assert len(results) == 2


def test_profile_all_sorted_by_throughput_ascending():
    metrics = [
        make_metric(pipeline="fast_pipe", records=5000, duration_s=1.0),
        make_metric(pipeline="slow_pipe", records=10, duration_s=1.0),
    ]
    results = profile_all(metrics)
    assert results[0].pipeline == "slow_pipe"
    assert results[1].pipeline == "fast_pipe"


def test_format_profiled_empty():
    assert "No profiling" in format_profiled([])


def test_format_profiled_contains_pipeline_name():
    m = make_metric(pipeline="inventory", records=300, duration_s=1.0)
    results = profile_all([m])
    text = format_profiled(results)
    assert "inventory" in text


def test_to_dict_keys():
    m = make_metric(records=100, duration_s=1.0)
    r = profile_pipeline([m])
    d = r.to_dict()
    for key in ("pipeline", "sample_count", "avg_duration_s", "throughput_per_s", "grade"):
        assert key in d
