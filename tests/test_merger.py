"""Tests for pipewatch.merger."""
import time
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.merger import merge_sources, format_merged, MergedMetric


def make_metric(pipeline="etl", status="ok", total=100, failed=0, ts=None):
    return PipelineMetric(
        pipeline=pipeline,
        status=status,
        total_records=total,
        failed_records=failed,
        timestamp=ts or time.time(),
    )


def test_merge_empty_sources():
    result = merge_sources([])
    assert result == {}


def test_merge_single_source():
    metrics = [make_metric("a"), make_metric("b")]
    result = merge_sources([metrics])
    assert "a" in result
    assert "b" in result
    assert result["a"].source_count == 1


def test_merge_two_sources_same_pipeline():
    m1 = make_metric("pipe", ts=1000.0)
    m2 = make_metric("pipe", ts=2000.0)
    result = merge_sources([[m1], [m2]])
    assert "pipe" in result
    merged = result["pipe"]
    assert merged.metric_count == 2
    assert merged.source_count == 2


def test_merge_latest_returns_most_recent():
    m1 = make_metric("pipe", ts=1000.0)
    m2 = make_metric("pipe", ts=3000.0)
    result = merge_sources([[m1, m2]])
    assert result["pipe"].latest().timestamp == 3000.0


def test_merge_disjoint_pipelines():
    source1 = [make_metric("alpha")]
    source2 = [make_metric("beta")]
    result = merge_sources([source1, source2])
    assert result["alpha"].source_count == 1
    assert result["beta"].source_count == 1


def test_to_dict_structure():
    m = make_metric("pipe", status="ok", total=200, failed=10)
    result = merge_sources([[m]])
    d = result["pipe"].to_dict()
    assert d["pipeline"] == "pipe"
    assert d["source_count"] == 1
    assert d["metric_count"] == 1
    assert d["latest"] is not None


def test_format_merged_empty():
    assert "No merged" in format_merged({})


def test_format_merged_shows_pipeline():
    m = make_metric("sales", status="ok", total=100, failed=5)
    result = merge_sources([[m]])
    text = format_merged(result)
    assert "sales" in text
    assert "ok" in text
