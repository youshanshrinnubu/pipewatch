"""Tests for pipewatch.clamper."""
from __future__ import annotations

import datetime

import pytest

from pipewatch.clamper import ClampConfig, ClampResult, clamp_all, clamp_metric
from pipewatch.metrics import PipelineMetric

NOW = datetime.datetime.utcnow()


def make_metric(
    name: str = "pipe",
    status: str = "ok",
    total: int = 100,
    failed: int = 0,
    duration: float | None = None,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        status=status,
        total_records=total,
        failed_records=failed,
        duration_seconds=duration,
        timestamp=NOW,
    )


# ---------------------------------------------------------------------------
# clamp_metric — no change when within bounds
# ---------------------------------------------------------------------------

def test_no_change_when_within_bounds():
    m = make_metric(total=500, failed=10)
    cfg = ClampConfig(max_total_records=1000, max_failed_records=100)
    result = clamp_metric(m, cfg)
    assert not result.changed
    assert result.fields_changed == []
    assert result.clamped.total_records == 500
    assert result.clamped.failed_records == 10


def test_clamps_total_records_above_max():
    m = make_metric(total=5_000_000)
    cfg = ClampConfig(max_total_records=1_000_000)
    result = clamp_metric(m, cfg)
    assert result.changed
    assert "total_records" in result.fields_changed
    assert result.clamped.total_records == 1_000_000


def test_clamps_failed_records_above_max():
    m = make_metric(total=100, failed=200)
    cfg = ClampConfig(max_failed_records=100)
    result = clamp_metric(m, cfg)
    assert result.changed
    assert "failed_records" in result.fields_changed
    assert result.clamped.failed_records == 100


def test_clamps_failed_records_below_min():
    # Negative failed_records should be floored to 0
    m = make_metric(failed=0)
    # Manually patch for the test
    m = PipelineMetric("p", "ok", 100, -5, None, NOW)
    cfg = ClampConfig(min_failed_records=0)
    result = clamp_metric(m, cfg)
    assert result.changed
    assert result.clamped.failed_records == 0


def test_clamps_duration_above_max():
    m = make_metric(duration=9999.0)
    cfg = ClampConfig(max_duration_seconds=300.0)
    result = clamp_metric(m, cfg)
    assert result.changed
    assert "duration_seconds" in result.fields_changed
    assert result.clamped.duration_seconds == 300.0


def test_duration_none_not_clamped():
    m = make_metric(duration=None)
    cfg = ClampConfig(max_duration_seconds=300.0)
    result = clamp_metric(m, cfg)
    assert "duration_seconds" not in result.fields_changed


def test_multiple_fields_clamped():
    m = make_metric(total=99_999_999, failed=99_999_999, duration=86400.0)
    cfg = ClampConfig(
        max_total_records=1_000,
        max_failed_records=500,
        max_duration_seconds=3600.0,
    )
    result = clamp_metric(m, cfg)
    assert set(result.fields_changed) == {"total_records", "failed_records", "duration_seconds"}


def test_clamp_all_returns_one_result_per_metric():
    metrics = [make_metric(name=f"p{i}", total=i * 1000) for i in range(5)]
    cfg = ClampConfig(max_total_records=2000)
    results = clamp_all(metrics, cfg)
    assert len(results) == 5


def test_to_dict_contains_expected_keys():
    m = make_metric(total=200, failed=50)
    cfg = ClampConfig(max_total_records=100)
    result = clamp_metric(m, cfg)
    d = result.to_dict()
    assert d["pipeline"] == "pipe"
    assert d["changed"] is True
    assert "original" in d
    assert "clamped" in d
    assert d["original"]["total_records"] == 200
    assert d["clamped"]["total_records"] == 100


def test_pipeline_name_preserved_after_clamp():
    m = make_metric(name="critical_pipeline", total=9_999_999)
    cfg = ClampConfig(max_total_records=100)
    result = clamp_metric(m, cfg)
    assert result.clamped.pipeline_name == "critical_pipeline"
