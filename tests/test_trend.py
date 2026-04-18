"""Tests for pipewatch.trend module."""
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.trend import analyze_trend, analyze_all_trends, TrendResult


def make_metric(pipeline="pipe", total=100, failed=0, status="ok"):
    return PipelineMetric(
        pipeline_name=pipeline,
        total_records=total,
        failed_records=failed,
        status=status,
        duration_seconds=1.0,
    )


def test_analyze_trend_returns_none_for_empty():
    assert analyze_trend([]) is None


def test_analyze_trend_single_metric_stable():
    result = analyze_trend([make_metric(failed=5)])
    assert result.direction == "stable"
    assert result.sample_count == 1


def test_analyze_trend_degrading():
    metrics = [
        make_metric(failed=1),
        make_metric(failed=2),
        make_metric(failed=20),
        make_metric(failed=25),
    ]
    result = analyze_trend(metrics, threshold=0.05)
    assert result.direction == "degrading"


def test_analyze_trend_improving():
    metrics = [
        make_metric(failed=25),
        make_metric(failed=20),
        make_metric(failed=2),
        make_metric(failed=1),
    ]
    result = analyze_trend(metrics, threshold=0.05)
    assert result.direction == "improving"


def test_analyze_trend_stable_within_threshold():
    metrics = [make_metric(failed=5), make_metric(failed=6)]
    result = analyze_trend(metrics, threshold=0.10)
    assert result.direction == "stable"


def test_analyze_trend_stats():
    metrics = [make_metric(failed=10), make_metric(failed=20)]
    result = analyze_trend(metrics)
    assert result.min_failure_rate == pytest.approx(0.10)
    assert result.max_failure_rate == pytest.approx(0.20)
    assert result.avg_failure_rate == pytest.approx(0.15)


def test_analyze_all_trends_groups_by_pipeline():
    metrics = [
        make_metric(pipeline="a", failed=1),
        make_metric(pipeline="b", failed=50),
        make_metric(pipeline="a", failed=2),
        make_metric(pipeline="b", failed=60),
    ]
    results = analyze_all_trends(metrics)
    pipelines = {r.pipeline for r in results}
    assert pipelines == {"a", "b"}


def test_to_dict_keys():
    result = analyze_trend([make_metric(failed=5)])
    d = result.to_dict()
    assert set(d.keys()) == {"pipeline", "direction", "avg_failure_rate", "min_failure_rate", "max_failure_rate", "sample_count"}
