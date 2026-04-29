"""Tests for pipewatch.freezer."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.freezer import FreezeRecord, FreezeStore, format_freeze_record


def make_metric(name: str, status: str = "ok", total: int = 100, failed: int = 0) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        status=status,
        total_records=total,
        failed_records=failed,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@pytest.fixture
def store(tmp_path):
    return FreezeStore(str(tmp_path / "freeze.json"))


def test_freeze_creates_record(store):
    m = make_metric("etl")
    record = store.freeze("v1", [m])
    assert record.label == "v1"
    assert len(record.metrics) == 1
    assert record.metrics[0].pipeline_name == "etl"


def test_thaw_returns_frozen_record(store):
    m = make_metric("etl")
    store.freeze("v1", [m])
    record = store.thaw("v1")
    assert record is not None
    assert record.label == "v1"


def test_thaw_returns_none_for_unknown(store):
    assert store.thaw("missing") is None


def test_list_labels_empty(store):
    assert store.list_labels() == []


def test_list_labels_after_freeze(store):
    store.freeze("a", [make_metric("p1")])
    store.freeze("b", [make_metric("p2")])
    labels = store.list_labels()
    assert "a" in labels
    assert "b" in labels


def test_delete_removes_label(store):
    store.freeze("v1", [make_metric("p")])
    result = store.delete("v1")
    assert result is True
    assert store.thaw("v1") is None


def test_delete_missing_returns_false(store):
    assert store.delete("ghost") is False


def test_persistence_roundtrip(tmp_path):
    path = str(tmp_path / "freeze.json")
    s1 = FreezeStore(path)
    s1.freeze("snap1", [make_metric("pipeline_a")])
    s2 = FreezeStore(path)
    record = s2.thaw("snap1")
    assert record is not None
    assert record.metrics[0].pipeline_name == "pipeline_a"


def test_to_dict_roundtrip():
    m = make_metric("p")
    rec = FreezeRecord(label="x", frozen_at="2024-01-01T00:00:00+00:00", metrics=[m])
    d = rec.to_dict()
    restored = FreezeRecord.from_dict(d)
    assert restored.label == "x"
    assert restored.metrics[0].pipeline_name == "p"


def test_format_freeze_record_contains_label():
    m = make_metric("etl", status="ok", total=200)
    rec = FreezeRecord(label="release-1", frozen_at="2024-06-01T12:00:00+00:00", metrics=[m])
    text = format_freeze_record(rec)
    assert "release-1" in text
    assert "etl" in text
    assert "ok" in text


def test_freeze_record_frozen_at_is_set(store):
    """Verify that freeze() populates frozen_at with a valid ISO 8601 timestamp."""
    before = datetime.now(timezone.utc)
    record = store.freeze("ts-check", [make_metric("p")])
    after = datetime.now(timezone.utc)

    frozen_at = datetime.fromisoformat(record.frozen_at)
    assert before <= frozen_at <= after
