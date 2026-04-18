"""Tests for pipewatch.metrics module."""

import pytest
from pipewatch.metrics import MetricsCollector, PipelineMetric


def make_metric(name="etl_main", status="ok", processed=100, failed=0, **kwargs):
    return PipelineMetric(
        pipeline_name=name,
        status=status,
        records_processed=processed,
        records_failed=failed,
        **kwargs,
    )


def test_failure_rate_zero_when_no_records():
    m = make_metric(processed=0, failed=0)
    assert m.failure_rate == 0.0


def test_failure_rate_calculated_correctly():
    m = make_metric(processed=90, failed=10)
    assert m.failure_rate == pytest.approx(0.1)


def test_is_healthy_ok_status_low_failures():
    m = make_metric(status="ok", processed=99, failed=1)
    assert m.is_healthy(max_failure_rate=0.05) is True


def test_is_healthy_fails_on_high_failure_rate():
    m = make_metric(status="ok", processed=80, failed=20)
    assert m.is_healthy(max_failure_rate=0.05) is False


def test_is_healthy_fails_on_error_status():
    m = make_metric(status="error", processed=100, failed=0)
    assert m.is_healthy() is False


def test_to_dict_contains_expected_keys():
    m = make_metric()
    d = m.to_dict()
    for key in ("pipeline_name", "status", "records_processed", "failure_rate", "timestamp"):
        assert key in d


def test_collector_records_and_retrieves_latest():
    collector = MetricsCollector()
    collector.record(make_metric(name="pipe_a", processed=50))
    collector.record(make_metric(name="pipe_a", processed=75))
    latest = collector.latest("pipe_a")
    assert latest is not None
    assert latest.records_processed == 75


def test_collector_returns_none_for_unknown_pipeline():
    collector = MetricsCollector()
    assert collector.latest("nonexistent") is None


def test_collector_evicts_oldest_when_over_capacity():
    collector = MetricsCollector(max_history=3)
    for i in range(4):
        collector.record(make_metric(name=f"pipe_{i}"))
    assert len(collector._history) == 3
    assert collector._history[0].pipeline_name == "pipe_1"


def test_collector_all_pipelines_unique():
    collector = MetricsCollector()
    collector.record(make_metric(name="a"))
    collector.record(make_metric(name="b"))
    collector.record(make_metric(name="a"))
    assert sorted(collector.all_pipelines()) == ["a", "b"]


def test_collector_history_filters_by_name():
    collector = MetricsCollector()
    collector.record(make_metric(name="x"))
    collector.record(make_metric(name="y"))
    collector.record(make_metric(name="x"))
    assert len(collector.history("x")) == 2
    assert len(collector.history("y")) == 1
