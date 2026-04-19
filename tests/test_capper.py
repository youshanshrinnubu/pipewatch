"""Tests for pipewatch.capper."""
import pytest
from datetime import datetime
from pipewatch.metrics import PipelineMetric
from pipewatch.capper import CapConfig, cap_metric, cap_all, format_capped


def make_metric(pipeline="pipe", total=1000, failed=100, status="ok") -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=pipeline,
        status=status,
        total_records=total,
        failed_records=failed,
        duration_seconds=1.0,
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
    )


def test_cap_metric_no_change_when_within_limits():
    m = make_metric(total=1000, failed=100)
    config = CapConfig(max_failure_rate=1.0)
    result = cap_metric(m, config)
    assert not result.changed
    assert result.capped.failed_records == 100
    assert result.capped.total_records == 1000


def test_cap_metric_clamps_failure_rate():
    m = make_metric(total=1000, failed=900)
    config = CapConfig(max_failure_rate=0.5)
    result = cap_metric(m, config)
    assert result.changed
    assert result.capped.failed_records <= 500


def test_cap_metric_clamps_total_records():
    m = make_metric(total=5000, failed=100)
    config = CapConfig(max_total_records=2000)
    result = cap_metric(m, config)
    assert result.changed
    assert result.capped.total_records == 2000


def test_cap_metric_clamps_failed_records():
    m = make_metric(total=1000, failed=800)
    config = CapConfig(max_failed_records=200)
    result = cap_metric(m, config)
    assert result.changed
    assert result.capped.failed_records == 200


def test_cap_metric_failed_not_exceeds_total_after_clamp():
    m = make_metric(total=100, failed=80)
    config = CapConfig(max_total_records=50, max_failure_rate=0.5)
    result = cap_metric(m, config)
    assert result.capped.failed_records <= result.capped.total_records


def test_cap_metric_preserves_pipeline_name():
    m = make_metric(pipeline="my_pipe")
    result = cap_metric(m, CapConfig())
    assert result.capped.pipeline_name == "my_pipe"


def test_cap_metric_zero_total():
    m = make_metric(total=0, failed=0)
    result = cap_metric(m, CapConfig(max_failure_rate=0.5))
    assert result.capped.failed_records == 0
    assert not result.changed


def test_cap_all_returns_one_result_per_metric():
    metrics = [make_metric(pipeline=f"p{i}") for i in range(4)]
    results = cap_all(metrics, CapConfig(max_failure_rate=0.2))
    assert len(results) == 4


def test_format_capped_empty():
    assert format_capped([]) == "No metrics to cap."


def test_format_capped_contains_pipeline_name():
    m = make_metric(pipeline="sales", total=1000, failed=600)
    results = cap_all([m], CapConfig(max_failure_rate=0.3))
    output = format_capped(results)
    assert "sales" in output


def test_format_capped_marks_changed():
    m = make_metric(total=1000, failed=900)
    results = cap_all([m], CapConfig(max_failure_rate=0.1))
    output = format_capped(results)
    assert "[changed]" in output


def test_format_capped_marks_unchanged():
    m = make_metric(total=1000, failed=50)
    results = cap_all([m], CapConfig(max_failure_rate=0.9))
    output = format_capped(results)
    assert "[unchanged]" in output
