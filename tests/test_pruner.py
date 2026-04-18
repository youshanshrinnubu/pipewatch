"""Tests for pipewatch.pruner."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.pruner import prune_by_age, prune_by_count, prune_store, PruneResult


def make_metric(pipeline="pipe", offset_seconds=0, status="ok", failure_rate=0.0):
    return PipelineMetric(
        pipeline=pipeline,
        timestamp=datetime.utcnow() - timedelta(seconds=offset_seconds),
        total_records=100,
        failed_records=int(failure_rate * 100),
        status=status,
    )


def test_prune_by_age_keeps_recent():
    metrics = [make_metric(offset_seconds=10), make_metric(offset_seconds=200)]
    result = prune_by_age(metrics, max_age_seconds=60)
    assert len(result) == 1
    assert result[0].timestamp > datetime.utcnow() - timedelta(seconds=60)


def test_prune_by_age_keeps_all_when_recent():
    metrics = [make_metric(offset_seconds=5), make_metric(offset_seconds=10)]
    result = prune_by_age(metrics, max_age_seconds=60)
    assert len(result) == 2


def test_prune_by_count_trims_oldest():
    metrics = [make_metric(offset_seconds=30), make_metric(offset_seconds=10), make_metric(offset_seconds=20)]
    result = prune_by_count(metrics, max_count=2)
    assert len(result) == 2
    # should keep the 2 most recent
    offsets = sorted([int((datetime.utcnow() - m.timestamp).total_seconds()) for m in result])
    assert offsets[0] < 15


def test_prune_by_count_no_op_when_under_limit():
    metrics = [make_metric(), make_metric()]
    result = prune_by_count(metrics, max_count=5)
    assert len(result) == 2


def test_prune_store_returns_results(tmp_path):
    from pipewatch.snapshot import SnapshotStore
    store = SnapshotStore(path=str(tmp_path / "snap.json"))
    for i in range(5):
        store.save(make_metric(pipeline="alpha", offset_seconds=i * 10))
    results = prune_store(store, max_count=3)
    assert len(results) == 1
    r = results[0]
    assert r.pipeline == "alpha"
    assert r.removed == 2
    assert r.retained == 3


def test_prune_store_multiple_pipelines(tmp_path):
    from pipewatch.snapshot import SnapshotStore
    store = SnapshotStore(path=str(tmp_path / "snap.json"))
    for i in range(4):
        store.save(make_metric(pipeline="a", offset_seconds=i * 5))
        store.save(make_metric(pipeline="b", offset_seconds=i * 5))
    results = prune_store(store, max_count=2)
    assert len(results) == 2
    for r in results:
        assert r.retained == 2
        assert r.removed == 2


def test_prune_result_to_dict():
    r = PruneResult(pipeline="x", removed=3, retained=7)
    d = r.to_dict()
    assert d == {"pipeline": "x", "removed": 3, "retained": 7}
