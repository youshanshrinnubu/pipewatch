"""Tests for baseline store and violation detection."""
import json
import os
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.baseline import BaselineStore, BaselineViolation


def make_metric(name="pipe", total=100, failed=5, status="ok") -> PipelineMetric:
    return PipelineMetric(pipeline_name=name, total_records=total, failed_records=failed, status=status)


@pytest.fixture
def store(tmp_path):
    return BaselineStore(str(tmp_path / "baseline.json"))


def test_save_and_load_roundtrip(store):
    metrics = [make_metric("a"), make_metric("b", failed=10)]
    store.save(metrics)
    loaded = store.load()
    names = {m.pipeline_name for m in loaded}
    assert names == {"a", "b"}


def test_load_empty_when_no_file(store):
    assert store.load() == []


def test_check_no_violations_when_same(store):
    m = make_metric("pipe", total=100, failed=5)
    store.save([m])
    violations = store.check([make_metric("pipe", total=100, failed=5)])
    assert len(violations) == 1
    assert not violations[0].is_regression


def test_check_regression_on_higher_failure_rate(store):
    store.save([make_metric("pipe", total=100, failed=5)])
    current = [make_metric("pipe", total=100, failed=30)]
    violations = store.check(current)
    assert violations[0].is_regression


def test_check_regression_on_status_change(store):
    store.save([make_metric("pipe", status="ok")])
    current = [make_metric("pipe", status="error")]
    violations = store.check(current)
    assert violations[0].is_regression


def test_check_skips_unknown_pipeline(store):
    store.save([make_metric("known")])
    violations = store.check([make_metric("unknown")])
    assert violations == []


def test_violation_to_dict():
    v = BaselineViolation(
        pipeline="p",
        baseline_failure_rate=0.05,
        current_failure_rate=0.20,
        baseline_status="ok",
        current_status="ok",
    )
    d = v.to_dict()
    assert d["pipeline"] == "p"
    assert d["is_regression"] is True
    assert "baseline_failure_rate" in d


def test_improvement_not_regression(store):
    store.save([make_metric("pipe", total=100, failed=30)])
    current = [make_metric("pipe", total=100, failed=5)]
    violations = store.check(current)
    assert not violations[0].is_regression
