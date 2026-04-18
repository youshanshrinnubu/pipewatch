"""Tests for pipewatch.aggregator."""
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.aggregator import aggregate, aggregate_all, format_aggregated


def make_metric(pipeline="pipe", records_processed=100, records_failed=0, status="ok"):
    return PipelineMetric(
        pipeline=pipeline,
        records_processed=records_processed,
        records_failed=records_failed,
        status=status,
        duration_seconds=1.0,
    )


def test_aggregate_returns_none_for_empty():
    assert aggregate([]) is None


def test_aggregate_single_metric():
    m = make_metric(records_processed=200, records_failed=10)
    stats = aggregate([m])
    assert stats is not None
    assert stats.total_runs == 1
    assert stats.avg_failure_rate == pytest.approx(10 / 200)
    assert stats.max_failure_rate == pytest.approx(10 / 200)
    assert stats.ok_count == 1
    assert stats.error_count == 0


def test_aggregate_multiple_metrics():
    metrics = [
        make_metric(records_processed=100, records_failed=10, status="ok"),
        make_metric(records_processed=100, records_failed=30, status="error"),
    ]
    stats = aggregate(metrics)
    assert stats.total_runs == 2
    assert stats.avg_failure_rate == pytest.approx(0.2)
    assert stats.max_failure_rate == pytest.approx(0.3)
    assert stats.ok_count == 1
    assert stats.error_count == 1


def test_aggregate_avg_records():
    metrics = [
        make_metric(records_processed=100),
        make_metric(records_processed=200),
    ]
    stats = aggregate(metrics)
    assert stats.avg_records_processed == pytest.approx(150.0)


def test_aggregate_all_groups_by_pipeline():
    metrics = [
        make_metric(pipeline="a", records_processed=100),
        make_metric(pipeline="b", records_processed=50),
        make_metric(pipeline="a", records_processed=200),
    ]
    result = aggregate_all(metrics)
    assert set(result.keys()) == {"a", "b"}
    assert result["a"].total_runs == 2
    assert result["b"].total_runs == 1


def test_aggregate_all_empty():
    assert aggregate_all([]) == {}


def test_format_aggregated_contains_pipeline_name():
    m = make_metric(pipeline="my_pipe", records_processed=100, records_failed=5)
    stats = aggregate([m])
    output = format_aggregated(stats)
    assert "my_pipe" in output
    assert "Runs" in output
    assert "Avg Fail" in output
