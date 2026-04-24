"""Tests for pipewatch.segmenter."""

from __future__ import annotations

from datetime import datetime

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.segmenter import (
    DEFAULT_SEGMENTS,
    Segment,
    SegmentResult,
    format_segmented,
    segment,
)


def make_metric(
    name: str = "pipe",
    total: int = 100,
    failed: int = 0,
    status: str = "ok",
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        total_records=total,
        failed_records=failed,
        status=status,
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
    )


def test_segment_empty_metrics():
    result = segment([])
    assert isinstance(result, SegmentResult)
    for seg in result.segments.values():
        assert len(seg.metrics) == 0


def test_segment_healthy_metric_goes_to_healthy():
    m = make_metric(total=100, failed=2)  # 2% failure rate
    result = segment([m])
    assert m in result.segments["healthy"].metrics
    assert m not in result.segments["degraded"].metrics
    assert m not in result.segments["critical"].metrics


def test_segment_degraded_metric():
    m = make_metric(total=100, failed=10)  # 10% failure rate
    result = segment([m])
    assert m in result.segments["degraded"].metrics
    assert m not in result.segments["healthy"].metrics


def test_segment_critical_metric():
    m = make_metric(total=100, failed=50)  # 50% failure rate
    result = segment([m])
    assert m in result.segments["critical"].metrics


def test_segment_zero_records_goes_to_healthy():
    m = make_metric(total=0, failed=0)
    result = segment([m])
    assert m in result.segments["healthy"].metrics


def test_segment_multiple_metrics_distributed():
    metrics = [
        make_metric("a", total=100, failed=1),   # healthy
        make_metric("b", total=100, failed=15),  # degraded
        make_metric("c", total=100, failed=40),  # critical
    ]
    result = segment(metrics)
    assert len(result.segments["healthy"].metrics) == 1
    assert len(result.segments["degraded"].metrics) == 1
    assert len(result.segments["critical"].metrics) == 1


def test_segment_to_dict_contains_count():
    m = make_metric(total=100, failed=3)
    result = segment([m])
    d = result.to_dict()
    assert d["healthy"]["count"] == 1
    assert d["degraded"]["count"] == 0


def test_segment_to_dict_contains_pipelines():
    m = make_metric(name="my_pipe", total=100, failed=1)
    result = segment([m])
    d = result.to_dict()
    assert "my_pipe" in d["healthy"]["pipelines"]


def test_segment_get_returns_correct_segment():
    result = segment([])
    seg = result.get("healthy")
    assert seg is not None
    assert seg.name == "healthy"


def test_segment_get_returns_none_for_unknown():
    result = segment([])
    assert result.get("nonexistent") is None


def test_format_segmented_contains_segment_names():
    m = make_metric(total=100, failed=5)
    result = segment([m])
    text = format_segmented(result)
    assert "healthy" in text
    assert "degraded" in text
    assert "critical" in text


def test_format_segmented_contains_pipeline_name():
    m = make_metric(name="sales_etl", total=100, failed=1)
    result = segment([m])
    text = format_segmented(result)
    assert "sales_etl" in text


def test_custom_segments():
    custom = [("low", 0.0, 0.1), ("high", 0.1, 1.01)]
    m_low = make_metric("a", total=100, failed=5)
    m_high = make_metric("b", total=100, failed=30)
    result = segment([m_low, m_high], segments=custom)
    assert m_low in result.segments["low"].metrics
    assert m_high in result.segments["high"].metrics
