"""Tests for pipewatch/compactor.py"""
from __future__ import annotations

import datetime
from typing import List

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.compactor import compact, format_compacted, CompactResult


def make_metric(
    pipeline: str = "pipe",
    status: str = "ok",
    total: int = 100,
    failed: int = 0,
    ts: float = 1_000_000.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=pipeline,
        status=status,
        total_records=total,
        failed_records=failed,
        timestamp=ts,
    )


def test_compact_empty_returns_empty():
    result, info = compact([])
    assert result == []
    assert info == []


def test_compact_single_metric_unchanged():
    m = make_metric()
    result, info = compact([m])
    assert len(result) == 1
    assert result[0] is m
    assert info[0].before_count == 1
    assert info[0].removed == 0


def test_compact_keeps_most_recent():
    old = make_metric(pipeline="pipe", ts=1000.0)
    new = make_metric(pipeline="pipe", ts=2000.0)
    result, info = compact([old, new])
    assert len(result) == 1
    assert result[0].timestamp == 2000.0
    assert info[0].before_count == 2
    assert info[0].after_count == 1
    assert info[0].removed == 1


def test_compact_multiple_pipelines_kept_separately():
    a1 = make_metric(pipeline="a", ts=1000.0)
    a2 = make_metric(pipeline="a", ts=2000.0)
    b1 = make_metric(pipeline="b", ts=500.0)
    result, info = compact([a1, a2, b1])
    assert len(result) == 2
    pipelines = {m.pipeline_name for m in result}
    assert pipelines == {"a", "b"}


def test_compact_result_to_dict():
    m = make_metric(ts=9999.0)
    r = CompactResult(pipeline="pipe", before_count=3, after_count=1, kept=m)
    d = r.to_dict()
    assert d["pipeline"] == "pipe"
    assert d["before_count"] == 3
    assert d["after_count"] == 1
    assert d["removed"] == 2
    assert d["kept_timestamp"] == 9999.0


def test_format_compacted_empty():
    out = format_compacted([])
    assert "No pipelines" in out


def test_format_compacted_shows_removed_count():
    m = make_metric(ts=1.0)
    r = CompactResult(pipeline="sales", before_count=5, after_count=1, kept=m)
    out = format_compacted([r])
    assert "sales" in out
    assert "removed 4" in out
    assert "Total removed: 4" in out


def test_compact_three_duplicates_keeps_newest():
    metrics = [
        make_metric(pipeline="etl", ts=float(i)) for i in range(5)
    ]
    result, info = compact(metrics)
    assert len(result) == 1
    assert result[0].timestamp == 4.0
    assert info[0].removed == 4
