import time
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.patcher import PatchRule, patch_metric, patch_all, format_patch_results


def make_metric(pipeline="sales", status="ok", total=1000, failed=10):
    return PipelineMetric(pipeline, status, total, failed, time.time())


def test_patch_no_rules_unchanged():
    m = make_metric()
    result = patch_metric(m, [])
    assert not result.changed
    assert result.rules_applied == 0


def test_patch_sets_status():
    m = make_metric(status="ok")
    rule = PatchRule(pipeline="sales", set_status="error")
    result = patch_metric(m, [rule])
    assert result.changed
    assert result.patched.status == "error"
    assert result.rules_applied == 1


def test_patch_skips_non_matching_pipeline():
    m = make_metric(pipeline="orders")
    rule = PatchRule(pipeline="sales", set_status="error")
    result = patch_metric(m, [rule])
    assert not result.changed


def test_patch_none_pipeline_matches_all():
    m = make_metric(pipeline="anything")
    rule = PatchRule(pipeline=None, set_status="warning")
    result = patch_metric(m, [rule])
    assert result.patched.status == "warning"


def test_patch_sets_failure_rate():
    m = make_metric(total=1000, failed=10)
    rule = PatchRule(pipeline="sales", set_failure_rate=0.5)
    result = patch_metric(m, [rule])
    assert result.patched.failed_records == 500


def test_patch_all_applies_to_each():
    metrics = [make_metric("a"), make_metric("b"), make_metric("c")]
    rule = PatchRule(pipeline=None, set_status="error")
    results = patch_all(metrics, [rule])
    assert len(results) == 3
    assert all(r.patched.status == "error" for r in results)


def test_patch_multiple_rules_applied_in_order():
    m = make_metric(status="ok")
    rules = [
        PatchRule(pipeline="sales", set_status="warning"),
        PatchRule(pipeline="sales", set_status="error"),
    ]
    result = patch_metric(m, rules)
    assert result.patched.status == "error"
    assert result.rules_applied == 2


def test_format_patch_results_changed():
    m = make_metric(status="ok")
    rule = PatchRule(pipeline="sales", set_status="error")
    results = patch_all([m], [rule])
    text = format_patch_results(results)
    assert "CHANGED" in text
    assert "sales" in text


def test_format_patch_results_empty():
    text = format_patch_results([])
    assert "No metrics" in text


def test_to_dict_contains_pipeline():
    m = make_metric()
    rule = PatchRule(pipeline="sales", set_status="error")
    result = patch_metric(m, [rule])
    d = result.to_dict()
    assert d["pipeline"] == "sales"
    assert d["changed"] is True
