"""Tests for pipewatch.pinpointer."""
from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.pinpointer import (
    PinpointResult,
    pinpoint,
    pinpoint_all,
    to_dict,
)


def make_metric(
    pipeline="pipe",
    status="ok",
    total=100,
    failed=0,
    duration=1.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        status=status,
        total_records=total,
        failed_records=failed,
        duration_seconds=duration,
    )


def test_pinpoint_returns_none_for_empty():
    assert pinpoint("pipe", []) is None


def test_pinpoint_returns_none_for_unknown_pipeline():
    m = make_metric(pipeline="other")
    assert pinpoint("pipe", [m]) is None


def test_pinpoint_single_metric():
    m = make_metric(pipeline="pipe", status="ok", total=100, failed=0)
    r = pinpoint("pipe", [m])
    assert r is not None
    assert r.pipeline == "pipe"
    assert r.worst_metric is m
    assert r.score == pytest.approx(1.0)


def test_pinpoint_picks_worst_by_failure_rate():
    good = make_metric(pipeline="pipe", total=100, failed=5)
    bad = make_metric(pipeline="pipe", total=100, failed=60)
    r = pinpoint("pipe", [good, bad])
    assert r is not None
    assert r.worst_metric is bad


def test_pinpoint_error_status_is_worst():
    warning = make_metric(pipeline="pipe", status="warning", total=100, failed=30)
    error = make_metric(pipeline="pipe", status="error", total=100, failed=10)
    r = pinpoint("pipe", [warning, error])
    assert r is not None
    assert r.worst_metric is error
    assert r.score == pytest.approx(0.0)


def test_pinpoint_reason_error_status():
    m = make_metric(status="error")
    r = pinpoint("pipe", [m])
    assert "error" in r.reason


def test_pinpoint_reason_high_failure_rate():
    m = make_metric(total=100, failed=55)
    r = pinpoint("pipe", [m])
    assert "critically high" in r.reason


def test_pinpoint_reason_warning_status():
    m = make_metric(status="warning", total=100, failed=5)
    r = pinpoint("pipe", [m])
    assert "warning" in r.reason


def test_pinpoint_all_returns_all_pipelines():
    metrics = [
        make_metric(pipeline="a"),
        make_metric(pipeline="b", status="error"),
    ]
    results = pinpoint_all(metrics)
    assert set(results.keys()) == {"a", "b"}


def test_pinpoint_all_empty():
    assert pinpoint_all([]) == {}


def test_to_dict_keys():
    m = make_metric()
    r = PinpointResult(pipeline="pipe", worst_metric=m, reason="ok", score=0.95)
    d = to_dict(r)
    assert "pipeline" in d
    assert "score" in d
    assert "reason" in d
    assert "metric" in d
