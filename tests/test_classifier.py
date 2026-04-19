"""Tests for pipewatch.classifier."""
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.classifier import classify, classify_all, format_classified, ClassifiedMetric


def make_metric(name="pipe", total=100, failed=0, status="ok"):
    return PipelineMetric(name, total, failed, status)


def test_classify_healthy_low_failure():
    m = make_metric(failed=5)
    result = classify(m)
    assert result.tier == "healthy"
    assert result.reasons == []


def test_classify_degraded_moderate_failure():
    m = make_metric(failed=15)
    result = classify(m)
    assert result.tier == "degraded"
    assert any("15.0%" in r for r in result.reasons)


def test_classify_critical_high_failure():
    m = make_metric(failed=55)
    result = classify(m)
    assert result.tier == "critical"


def test_classify_critical_on_error_status():
    m = make_metric(status="error")
    result = classify(m)
    assert result.tier == "critical"
    assert any("error" in r for r in result.reasons)


def test_classify_unknown_when_no_records():
    m = make_metric(total=0, failed=0)
    result = classify(m)
    assert result.tier == "unknown"


def test_classify_degraded_non_ok_status():
    m = make_metric(status="warning", failed=0)
    result = classify(m)
    assert result.tier == "degraded"
    assert any("warning" in r for r in result.reasons)


def test_classify_all_returns_list():
    metrics = [make_metric(f"p{i}") for i in range(3)]
    results = classify_all(metrics)
    assert len(results) == 3
    assert all(isinstance(r, ClassifiedMetric) for r in results)


def test_to_dict_contains_fields():
    m = make_metric("alpha", failed=20)
    d = classify(m).to_dict()
    assert d["pipeline"] == "alpha"
    assert "tier" in d
    assert isinstance(d["reasons"], list)


def test_format_classified_empty():
    assert "No metrics" in format_classified([])


def test_format_classified_shows_tier():
    m = make_metric("beta", failed=60)
    output = format_classified([classify(m)])
    assert "CRITICAL" in output
    assert "beta" in output
