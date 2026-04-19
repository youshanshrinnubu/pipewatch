"""Tests for pipewatch.normalizer."""
from __future__ import annotations
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.normalizer import (
    normalize,
    normalize_all,
    format_normalized,
    NormalizedMetric,
)


def make_metric(
    pipeline="pipe_a",
    status="ok",
    total=100,
    failed=0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        status=status,
        total_records=total,
        failed_records=failed,
    )


def test_normalize_perfect_metric():
    m = make_metric(status="ok", total=100, failed=0)
    r = normalize(m)
    assert r.failure_rate == 0.0
    assert r.normalized_score == pytest.approx(1.0)


def test_normalize_error_status_is_zero():
    m = make_metric(status="error", total=100, failed=5)
    r = normalize(m)
    assert r.normalized_score == 0.0


def test_normalize_high_failure_rate():
    m = make_metric(status="ok", total=100, failed=80)
    r = normalize(m)
    assert r.failure_rate == pytest.approx(0.8)
    assert r.normalized_score == pytest.approx(0.2)


def test_normalize_warning_penalises_score():
    m_ok = make_metric(status="ok", total=100, failed=10)
    m_warn = make_metric(status="warning", total=100, failed=10)
    r_ok = normalize(m_ok)
    r_warn = normalize(m_warn)
    assert r_warn.normalized_score < r_ok.normalized_score


def test_normalize_zero_records():
    m = make_metric(total=0, failed=0)
    r = normalize(m)
    assert r.failure_rate == 0.0
    assert r.normalized_score == pytest.approx(1.0)


def test_normalize_all_returns_list():
    metrics = [make_metric(pipeline=f"p{i}") for i in range(4)]
    results = normalize_all(metrics)
    assert len(results) == 4
    assert all(isinstance(r, NormalizedMetric) for r in results)


def test_to_dict_keys():
    r = normalize(make_metric())
    d = r.to_dict()
    for key in ("pipeline", "status", "failure_rate", "normalized_score"):
        assert key in d


def test_format_normalized_empty():
    assert "No metrics" in format_normalized([])


def test_format_normalized_contains_pipeline():
    r = normalize(make_metric(pipeline="my_pipeline"))
    text = format_normalized([r])
    assert "my_pipeline" in text


def test_format_normalized_contains_score():
    r = normalize(make_metric(status="ok", total=100, failed=20))
    text = format_normalized([r])
    assert "0.8" in text or "80.00%" in text
