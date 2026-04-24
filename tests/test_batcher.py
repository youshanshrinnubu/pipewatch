"""Tests for pipewatch.batcher."""
from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.batcher import batch_metrics, format_batched


def make_metric(name: str, total: int = 100, failed: int = 0, status: str = "ok") -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        total_records=total,
        failed_records=failed,
        status=status,
    )


def test_batch_empty_metrics():
    result = batch_metrics([])
    assert result.total_metrics == 0
    assert result.count() == 0
    assert result.batches == []


def test_batch_single_metric():
    m = make_metric("sales")
    result = batch_metrics([m], batch_size=5)
    assert result.count() == 1
    assert result.batches[0].size() == 1
    assert result.batches[0].index == 0


def test_batch_exact_multiple():
    metrics = [make_metric(f"p{i}") for i in range(6)]
    result = batch_metrics(metrics, batch_size=3)
    assert result.count() == 2
    assert result.batches[0].size() == 3
    assert result.batches[1].size() == 3


def test_batch_remainder_creates_extra_batch():
    metrics = [make_metric(f"p{i}") for i in range(7)]
    result = batch_metrics(metrics, batch_size=3)
    assert result.count() == 3
    assert result.batches[2].size() == 1


def test_batch_indices_are_sequential():
    metrics = [make_metric(f"p{i}") for i in range(9)]
    result = batch_metrics(metrics, batch_size=4)
    for i, b in enumerate(result.batches):
        assert b.index == i


def test_batch_pipeline_filter():
    metrics = [
        make_metric("sales"),
        make_metric("inventory"),
        make_metric("sales"),
    ]
    result = batch_metrics(metrics, batch_size=5, pipeline="sales")
    assert result.total_metrics == 2
    assert all(n == "sales" for n in result.batches[0].pipeline_names())


def test_batch_pipeline_filter_no_match():
    metrics = [make_metric("sales")]
    result = batch_metrics(metrics, batch_size=5, pipeline="orders")
    assert result.total_metrics == 0
    assert result.count() == 0


def test_batch_invalid_size_raises():
    with pytest.raises(ValueError):
        batch_metrics([], batch_size=0)


def test_to_dict_structure():
    metrics = [make_metric(f"p{i}") for i in range(4)]
    result = batch_metrics(metrics, batch_size=2)
    d = result.to_dict()
    assert d["total_metrics"] == 4
    assert d["batch_size"] == 2
    assert d["batch_count"] == 2
    assert len(d["batches"]) == 2


def test_format_batched_contains_summary():
    metrics = [make_metric(f"pipe{i}") for i in range(3)]
    result = batch_metrics(metrics, batch_size=2)
    text = format_batched(result)
    assert "Batches" in text
    assert "Total" in text
    assert "pipe0" in text
