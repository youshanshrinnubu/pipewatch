"""Tests for snapshot persistence and summary statistics."""

import os
import pytest
from datetime import datetime

from pipewatch.metrics import PipelineMetric
from pipewatch.snapshot import SnapshotStore
from pipewatch.summary import summarize, summarize_all, format_summary


def make_metric(name="pipe", status="ok", processed=100, failed=5, duration=1.0):
    return PipelineMetric(
        pipeline_name=name,
        status=status,
        records_processed=processed,
        records_failed=failed,
        duration_seconds=duration,
        timestamp=datetime.utcnow(),
    )


@pytest.fixture
def store(tmp_path):
    return SnapshotStore(path=str(tmp_path / "snap.json"))


def test_save_and_load_roundtrip(store):
    metrics = [make_metric(), make_metric(status="error")]
    store.save(metrics)
    loaded = store.load()
    assert len(loaded) == 2
    assert loaded[0].pipeline_name == "pipe"
    assert loaded[1].status == "error"


def test_load_empty_when_no_file(store):
    assert store.load() == []


def test_save_appends_to_existing(store):
    store.save([make_metric()])
    store.save([make_metric()])
    assert len(store.load()) == 2


def test_max_entries_trimmed(tmp_path):
    store = SnapshotStore(path=str(tmp_path / "snap.json"), max_entries=3)
    store.save([make_metric()] * 5)
    assert len(store.load()) == 3


def test_load_for_pipeline_filters(store):
    store.save([make_metric(name="a"), make_metric(name="b"), make_metric(name="a")])
    result = store.load_for_pipeline("a")
    assert len(result) == 2
    assert all(m.pipeline_name == "a" for m in result)


def test_clear_removes_file(store):
    store.save([make_metric()])
    store.clear()
    assert not os.path.exists(store.path)


def test_summarize_basic():
    metrics = [make_metric(processed=100, failed=10), make_metric(processed=200, failed=20)]
    s = summarize(metrics)
    assert s is not None
    assert s.total_runs == 2
    assert s.avg_failure_rate == pytest.approx(0.1, rel=1e-3)
    assert s.error_run_count == 0


def test_summarize_counts_errors():
    metrics = [make_metric(status="error"), make_metric(status="ok")]
    s = summarize(metrics)
    assert s.error_run_count == 1
    assert s.last_status == "ok"


def test_summarize_all_groups_by_pipeline():
    metrics = [make_metric(name="a"), make_metric(name="b"), make_metric(name="a")]
    summaries = summarize_all(metrics)
    names = {s.pipeline_name for s in summaries}
    assert names == {"a", "b"}
    a = next(s for s in summaries if s.pipeline_name == "a")
    assert a.total_runs == 2


def test_format_summary_contains_name():
    s = summarize([make_metric(name="mypipe")])
    text = format_summary(s)
    assert "mypipe" in text
    assert "avg_failure_rate" in text
