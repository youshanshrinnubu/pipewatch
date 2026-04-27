"""Tests for pipewatch.sentinel."""
from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.sentinel import (
    SentinelRule,
    SentinelResult,
    check_sentinel,
    check_all_sentinels,
)


def make_metric(
    pipeline="pipe",
    total=1000,
    failed=0,
    status="ok",
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        total_records=total,
        failed_records=failed,
        status=status,
    )


def test_no_violation_when_within_limits():
    m = make_metric(total=100, failed=5)
    rule = SentinelRule(pipeline=None, max_failure_rate=0.1)
    results = check_sentinel(m, [rule])
    assert len(results) == 1
    assert not results[0].triggered


def test_violation_on_high_failure_rate():
    m = make_metric(total=100, failed=20)
    rule = SentinelRule(pipeline=None, max_failure_rate=0.1)
    results = check_sentinel(m, [rule])
    assert results[0].triggered
    assert any("failure rate" in v for v in results[0].violations)


def test_violation_on_max_failed_records():
    m = make_metric(total=1000, failed=50)
    rule = SentinelRule(pipeline=None, max_failed_records=10)
    results = check_sentinel(m, [rule])
    assert results[0].triggered
    assert any("failed records" in v for v in results[0].violations)


def test_violation_on_forbidden_status():
    m = make_metric(status="error")
    rule = SentinelRule(pipeline=None, forbidden_statuses=["error"])
    results = check_sentinel(m, [rule])
    assert results[0].triggered
    assert any("forbidden" in v for v in results[0].violations)


def test_rule_matches_specific_pipeline_only():
    m_sales = make_metric(pipeline="sales", total=100, failed=50)
    m_orders = make_metric(pipeline="orders", total=100, failed=50)
    rule = SentinelRule(pipeline="sales", max_failure_rate=0.1)
    assert check_sentinel(m_sales, [rule])[0].triggered
    assert not check_sentinel(m_orders, [rule])  # no matching rules


def test_rule_none_pipeline_matches_all():
    metrics = [make_metric(pipeline="a"), make_metric(pipeline="b")]
    rule = SentinelRule(pipeline=None, max_failure_rate=0.5)
    results = check_all_sentinels(metrics, [rule])
    assert len(results) == 2


def test_multiple_violations_reported():
    m = make_metric(total=100, failed=80, status="error")
    rule = SentinelRule(
        pipeline=None,
        max_failure_rate=0.1,
        max_failed_records=10,
        forbidden_statuses=["error"],
    )
    results = check_sentinel(m, [rule])
    assert len(results[0].violations) == 3


def test_to_dict_contains_pipeline_and_triggered():
    m = make_metric(total=100, failed=50)
    rule = SentinelRule(pipeline=None, max_failure_rate=0.1)
    result = check_sentinel(m, [rule])[0]
    d = result.to_dict()
    assert d["pipeline"] == "pipe"
    assert d["triggered"] is True


def test_to_alert_returns_alert_object():
    m = make_metric(total=100, failed=50)
    rule = SentinelRule(pipeline=None, max_failure_rate=0.1)
    result = check_sentinel(m, [rule])[0]
    alert = result.to_alert()
    assert alert.pipeline == "pipe"
    assert "Sentinel triggered" in alert.message


def test_zero_total_records_no_rate_violation():
    m = make_metric(total=0, failed=0)
    rule = SentinelRule(pipeline=None, max_failure_rate=0.05)
    results = check_sentinel(m, [rule])
    assert not results[0].triggered
