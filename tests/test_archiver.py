"""Tests for pipewatch.archiver."""

from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone

import pytest

from pipewatch.archiver import archive_all, archive_store, ArchiveResult
from pipewatch.metrics import PipelineMetric
from pipewatch.snapshot import SnapshotStore


def make_metric(pipeline: str = "test", records_failed: int = 0) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        status="ok",
        records_processed=100,
        records_failed=records_failed,
        duration_seconds=1.0,
        timestamp=datetime.now(timezone.utc).timestamp(),
    )


@pytest.fixture
def store(tmp_path):
    return SnapshotStore(path=str(tmp_path / "snap.json"))


def test_archive_returns_none_when_under_limit(store, tmp_path):
    for _ in range(5):
        store.save("sales", make_metric("sales"))
    result = archive_store(store, "sales", max_keep=10, archive_dir=str(tmp_path / "arch"))
    assert result is None


def test_archive_returns_result_when_over_limit(store, tmp_path):
    for _ in range(15):
        store.save("sales", make_metric("sales"))
    arch_dir = str(tmp_path / "arch")
    result = archive_store(store, "sales", max_keep=10, archive_dir=arch_dir)
    assert isinstance(result, ArchiveResult)
    assert result.metrics_archived == 5
    assert result.metrics_remaining == 10


def test_archive_creates_file(store, tmp_path):
    for _ in range(12):
        store.save("sales", make_metric("sales"))
    arch_dir = str(tmp_path / "arch")
    result = archive_store(store, "sales", max_keep=5, archive_dir=arch_dir)
    assert result is not None
    assert os.path.isfile(result.path)
    assert result.path.endswith(".json.gz")


def test_archive_all_handles_multiple_pipelines(store, tmp_path):
    for _ in range(12):
        store.save("sales", make_metric("sales"))
    for _ in range(12):
        store.save("inventory", make_metric("inventory"))
    arch_dir = str(tmp_path / "arch")
    results = archive_all(store, max_keep=5, archive_dir=arch_dir)
    assert len(results) == 2
    names = {r.path for r in results}
    assert len(names) == 2


def test_archive_all_skips_pipelines_under_limit(store, tmp_path):
    for _ in range(3):
        store.save("tiny", make_metric("tiny"))
    for _ in range(20):
        store.save("big", make_metric("big"))
    arch_dir = str(tmp_path / "arch")
    results = archive_all(store, max_keep=10, archive_dir=arch_dir)
    assert len(results) == 1
    assert results[0].metrics_archived == 10


def test_archive_result_to_dict(tmp_path):
    r = ArchiveResult(path="/tmp/x.json.gz", metrics_archived=5, metrics_remaining=10)
    d = r.to_dict()
    assert d["path"] == "/tmp/x.json.gz"
    assert d["metrics_archived"] == 5
    assert d["metrics_remaining"] == 10
