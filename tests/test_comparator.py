"""Tests for pipewatch.comparator."""

import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.comparator import compare_snapshots, format_comparison, ComparisonResult


def make_metric(pipeline="pipe", status="ok", total=100, failed=5):
    return PipelineMetric(pipeline=pipeline, status=status, total_records=total, failed_records=failed)


def test_compare_same_metrics_zero_delta():
    m = make_metric(failed=10, total=100)
    results = compare_snapshots([m], [m])
    assert len(results) == 1
    assert results[0].failure_rate_delta == pytest.approx(0.0)
    assert not results[0].status_changed


def test_compare_detects_failure_rate_increase():
    before = make_metric(failed=5, total=100)
    after = make_metric(failed=20, total=100)
    results = compare_snapshots([before], [after])
    assert results[0].failure_rate_delta == pytest.approx(0.15)


def test_compare_detects_status_change():
    before = make_metric(status="ok")
    after = make_metric(status="error")
    results = compare_snapshots([before], [after])
    assert results[0].status_changed is True


def test_compare_only_in_before():
    before = make_metric(pipeline="old_pipe")
    results = compare_snapshots([before], [])
    assert results[0].only_in_before is True
    assert results[0].only_in_after is False


def test_compare_only_in_after():
    after = make_metric(pipeline="new_pipe")
    results = compare_snapshots([], [after])
    assert results[0].only_in_after is True
    assert results[0].only_in_before is False


def test_compare_multiple_pipelines_sorted():
    before = [make_metric("b"), make_metric("a")]
    after = [make_metric("a"), make_metric("b")]
    results = compare_snapshots(before, after)
    assert [r.pipeline for r in results] == ["a", "b"]


def test_format_comparison_empty():
    assert format_comparison([]) == "No pipelines to compare."


def test_format_comparison_contains_pipeline_name():
    before = make_metric(pipeline="mypipe", failed=5, total=100)
    after = make_metric(pipeline="mypipe", failed=10, total=100)
    results = compare_snapshots([before], [after])
    output = format_comparison(results)
    assert "mypipe" in output
    assert "+5.00%" in output


def test_to_dict_keys():
    r = ComparisonResult(pipeline="p", before=make_metric(), after=make_metric())
    d = r.to_dict()
    assert "pipeline" in d
    assert "failure_rate_delta" in d
    assert "status_changed" in d
