"""Tests for pipewatch.trimmer."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.trimmer import TrimConfig, format_trimmed, trim, trim_all


def make_metric(
    pipeline: str = "pipe",
    total: int = 100,
    failed: int = 0,
    status: str = "ok",
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=pipeline,
        total_records=total,
        failed_records=failed,
        status=status,
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )


# --- trim() ---

def test_trim_empty_returns_empty_result():
    result = trim([], TrimConfig())
    assert result.kept == []
    assert result.removed == []
    assert result.reason == "empty"


def test_trim_keeps_all_when_no_constraints():
    metrics = [make_metric(total=10, failed=1), make_metric(total=50, failed=0)]
    result = trim(metrics, TrimConfig())
    assert len(result.kept) == 2
    assert len(result.removed) == 0
    assert result.reason == "none"


def test_trim_removes_below_min_records():
    metrics = [make_metric(total=5), make_metric(total=100)]
    result = trim(metrics, TrimConfig(min_total_records=10))
    assert len(result.kept) == 1
    assert len(result.removed) == 1
    assert result.kept[0].total_records == 100


def test_trim_removes_above_max_failure_rate():
    metrics = [
        make_metric(total=100, failed=80),  # 0.80 rate — removed
        make_metric(total=100, failed=10),  # 0.10 rate — kept
    ]
    result = trim(metrics, TrimConfig(max_failure_rate=0.5))
    assert len(result.kept) == 1
    assert len(result.removed) == 1
    assert result.removed[0].failed_records == 80


def test_trim_removes_wrong_status():
    metrics = [
        make_metric(status="ok"),
        make_metric(status="error"),
        make_metric(status="warning"),
    ]
    result = trim(metrics, TrimConfig(require_status=["ok"]))
    assert len(result.kept) == 1
    assert result.kept[0].status == "ok"
    assert len(result.removed) == 2


def test_trim_reason_is_threshold_when_any_removed():
    metrics = [make_metric(total=0), make_metric(total=50)]
    result = trim(metrics, TrimConfig(min_total_records=1))
    assert result.reason == "threshold"


# --- trim_all() ---

def test_trim_all_groups_by_pipeline():
    metrics = [
        make_metric(pipeline="a", total=5),
        make_metric(pipeline="a", total=100),
        make_metric(pipeline="b", total=3),
    ]
    results = trim_all(metrics, TrimConfig(min_total_records=10))
    assert len(results) == 2
    names = {r.pipeline for r in results}
    assert names == {"a", "b"}


def test_trim_all_empty_returns_empty():
    assert trim_all([], TrimConfig()) == []


# --- format_trimmed() ---

def test_format_trimmed_empty():
    assert format_trimmed([]) == "No trim results."


def test_format_trimmed_contains_pipeline_name():
    metrics = [make_metric(pipeline="sales", total=100)]
    results = trim_all(metrics, TrimConfig())
    text = format_trimmed(results)
    assert "sales" in text
    assert "kept=1" in text
    assert "removed=0" in text
