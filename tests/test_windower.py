"""Tests for pipewatch/windower.py."""
from datetime import datetime, timedelta

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.windower import window_metrics, window_all, WindowResult


def make_metric(
    pipeline: str = "etl",
    status: str = "ok",
    total: int = 100,
    failed: int = 0,
    ts: datetime = None,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        status=status,
        total_records=total,
        failed_records=failed,
        timestamp=ts or datetime.utcnow(),
    )


NOW = datetime(2024, 6, 1, 12, 0, 0)


def test_window_returns_none_for_empty():
    assert window_metrics([], "etl", 300, NOW) is None


def test_window_returns_none_for_unknown_pipeline():
    m = make_metric(pipeline="sales", ts=NOW)
    assert window_metrics([m], "etl", 300, NOW) is None


def test_window_excludes_metrics_outside_window():
    old = make_metric(ts=NOW - timedelta(seconds=400))
    recent = make_metric(ts=NOW - timedelta(seconds=100))
    result = window_metrics([old, recent], "etl", 300, NOW)
    assert result is not None
    assert result.metric_count == 1


def test_window_includes_metrics_at_boundary():
    boundary = make_metric(ts=NOW - timedelta(seconds=300))
    result = window_metrics([boundary], "etl", 300, NOW)
    assert result is not None
    assert result.metric_count == 1


def test_window_avg_failure_rate():
    m1 = make_metric(total=100, failed=10, ts=NOW - timedelta(seconds=10))
    m2 = make_metric(total=100, failed=30, ts=NOW - timedelta(seconds=20))
    result = window_metrics([m1, m2], "etl", 300, NOW)
    assert result is not None
    assert abs(result.avg_failure_rate - 0.2) < 1e-6


def test_window_max_min_failure_rate():
    m1 = make_metric(total=100, failed=5, ts=NOW - timedelta(seconds=10))
    m2 = make_metric(total=100, failed=50, ts=NOW - timedelta(seconds=20))
    result = window_metrics([m1, m2], "etl", 300, NOW)
    assert result.max_failure_rate == pytest.approx(0.5)
    assert result.min_failure_rate == pytest.approx(0.05)


def test_window_dominant_status_most_frequent():
    metrics = [
        make_metric(status="ok", ts=NOW - timedelta(seconds=10)),
        make_metric(status="ok", ts=NOW - timedelta(seconds=20)),
        make_metric(status="warning", ts=NOW - timedelta(seconds=30)),
    ]
    result = window_metrics(metrics, "etl", 300, NOW)
    assert result.dominant_status == "ok"


def test_window_earliest_and_latest():
    t1 = NOW - timedelta(seconds=200)
    t2 = NOW - timedelta(seconds=50)
    metrics = [make_metric(ts=t1), make_metric(ts=t2)]
    result = window_metrics(metrics, "etl", 300, NOW)
    assert result.earliest == t1
    assert result.latest == t2


def test_window_zero_records_gives_zero_failure_rate():
    m = make_metric(total=0, failed=0, ts=NOW - timedelta(seconds=10))
    result = window_metrics([m], "etl", 300, NOW)
    assert result.avg_failure_rate == 0.0


def test_window_all_returns_one_per_pipeline():
    metrics = [
        make_metric(pipeline="a", ts=NOW - timedelta(seconds=10)),
        make_metric(pipeline="b", ts=NOW - timedelta(seconds=20)),
        make_metric(pipeline="a", ts=NOW - timedelta(seconds=30)),
    ]
    results = window_all(metrics, 300, NOW)
    assert len(results) == 2
    names = {r.pipeline for r in results}
    assert names == {"a", "b"}


def test_window_all_empty_returns_empty():
    assert window_all([], 300, NOW) == []


def test_to_dict_contains_expected_keys():
    m = make_metric(ts=NOW - timedelta(seconds=10))
    result = window_metrics([m], "etl", 300, NOW)
    d = result.to_dict()
    for key in ("pipeline", "window_seconds", "metric_count", "avg_failure_rate",
                "max_failure_rate", "min_failure_rate", "dominant_status",
                "earliest", "latest"):
        assert key in d
