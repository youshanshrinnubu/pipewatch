"""Tests for pipewatch.leveler."""
from __future__ import annotations
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.leveler import (
    LevelConfig,
    LevelResult,
    level_metric,
    level_all,
    format_leveled,
)


def make_metric(
    pipeline: str = "pipe",
    total: int = 100,
    failed: int = 0,
    status: str = "ok",
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        total_records=total,
        failed_records=failed,
        status=status,
    )


def test_level_ok_when_healthy():
    m = make_metric(total=100, failed=2)
    r = level_metric(m)
    assert r.level == "ok"


def test_level_warning_on_moderate_failure_rate():
    m = make_metric(total=100, failed=7)
    r = level_metric(m)
    assert r.level == "warning"
    assert "failure_rate" in r.reason


def test_level_critical_on_high_failure_rate():
    m = make_metric(total=100, failed=25)
    r = level_metric(m)
    assert r.level == "critical"
    assert "failure_rate" in r.reason


def test_level_critical_on_error_status():
    m = make_metric(status="error")
    r = level_metric(m)
    assert r.level == "critical"
    assert "status" in r.reason


def test_level_warning_on_warning_status():
    m = make_metric(status="warning")
    r = level_metric(m)
    assert r.level == "warning"
    assert "status" in r.reason


def test_level_zero_records_is_ok():
    m = make_metric(total=0, failed=0)
    r = level_metric(m)
    assert r.level == "ok"


def test_level_custom_thresholds():
    cfg = LevelConfig(warning_failure_rate=0.30, critical_failure_rate=0.60)
    m = make_metric(total=100, failed=40)
    r = level_metric(m, cfg)
    assert r.level == "warning"


def test_level_all_returns_one_per_metric():
    metrics = [make_metric(f"p{i}") for i in range(4)]
    results = level_all(metrics)
    assert len(results) == 4


def test_level_all_empty():
    assert level_all([]) == []


def test_format_leveled_contains_pipeline_name():
    m = make_metric(pipeline="sales", total=100, failed=30)
    results = [level_metric(m)]
    text = format_leveled(results)
    assert "sales" in text


def test_format_leveled_empty():
    text = format_leveled([])
    assert "No metrics" in text


def test_to_dict_has_required_keys():
    m = make_metric()
    r = level_metric(m)
    d = r.to_dict()
    assert "pipeline" in d
    assert "level" in d
    assert "reason" in d
