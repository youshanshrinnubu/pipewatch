"""Tests for pipewatch.labeler."""
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.labeler import Labeler, LabelRule, LabeledMetric


def make_metric(pipeline="pipe", total=1000, failed=0, status="ok"):
    return PipelineMetric(pipeline, total_records=total, failed_records=failed, status=status)


def test_healthy_label_on_low_failure_rate():
    labeler = Labeler()
    lm = labeler.label(make_metric(failed=10))  # 1%
    assert lm.has_label("healthy")


def test_degraded_label_on_moderate_failure_rate():
    labeler = Labeler()
    lm = labeler.label(make_metric(failed=100))  # 10%
    assert lm.has_label("degraded")
    assert not lm.has_label("healthy")


def test_critical_label_on_high_failure_rate():
    labeler = Labeler()
    lm = labeler.label(make_metric(failed=300))  # 30%
    assert lm.has_label("critical")


def test_error_label_on_error_status():
    labeler = Labeler()
    lm = labeler.label(make_metric(failed=0, status="error"))
    assert lm.has_label("error")


def test_no_labels_when_no_rules_match():
    labeler = Labeler(rules=[LabelRule(label="x", statuses=["never"])])
    lm = labeler.label(make_metric())
    assert lm.labels == []


def test_label_all_returns_all():
    labeler = Labeler()
    metrics = [make_metric(f"p{i}") for i in range(4)]
    results = labeler.label_all(metrics)
    assert len(results) == 4


def test_to_dict_contains_expected_keys():
    labeler = Labeler()
    lm = labeler.label(make_metric())
    d = lm.to_dict()
    assert "pipeline" in d
    assert "labels" in d
    assert "failure_rate" in d
    assert "status" in d


def test_custom_rule_applied():
    rule = LabelRule(label="slow", min_failure_rate=0.0, max_failure_rate=1.0, statuses=["ok"])
    labeler = Labeler(rules=[rule])
    lm = labeler.label(make_metric(status="ok"))
    assert lm.has_label("slow")
