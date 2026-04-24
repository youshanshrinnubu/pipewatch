"""Tests for pipewatch/cycler.py"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.cycler import Cycler, CycleResult, CycleState
from pipewatch.metrics import PipelineMetric


def make_metric(name: str, status: str = "ok", total: int = 100, failed: int = 0) -> PipelineMetric:
    ts = datetime.now(timezone.utc).isoformat()
    return PipelineMetric(name, status, total, failed, ts)


def test_cycle_state_current_empty():
    state = CycleState()
    assert state.current() is None


def test_cycle_state_advance_wraps():
    state = CycleState(pipelines=["a", "b", "c"], index=2)
    state.advance()
    assert state.index == 0


def test_cycle_state_current_returns_pipeline():
    state = CycleState(pipelines=["alpha", "beta"], index=1)
    assert state.current() == "beta"


def test_cycler_load_sets_pipelines():
    c = Cycler()
    c.load([make_metric("a"), make_metric("b")])
    assert c.current() is not None
    assert c.current().pipeline in ("a", "b")


def test_cycler_current_returns_none_when_empty():
    c = Cycler()
    assert c.current() is None


def test_cycler_next_advances_pipeline():
    c = Cycler()
    c.load([make_metric("alpha"), make_metric("beta")])
    first = c.current().pipeline
    second = c.next().pipeline
    assert first != second


def test_cycler_wraps_around():
    c = Cycler()
    metrics = [make_metric("x"), make_metric("y")]
    c.load(metrics)
    names = [c.current().pipeline]
    for _ in range(3):
        names.append(c.next().pipeline)
    # Should repeat after 2 steps
    assert names[0] == names[2]


def test_cycler_peek_all_returns_all_pipelines():
    c = Cycler()
    c.load([make_metric("p1"), make_metric("p2"), make_metric("p3")])
    results = c.peek_all()
    assert len(results) == 3
    pipeline_names = {r.pipeline for r in results}
    assert pipeline_names == {"p1", "p2", "p3"}


def test_cycler_result_position_and_total():
    c = Cycler()
    c.load([make_metric("a"), make_metric("b"), make_metric("c")])
    result = c.current()
    assert result.total == 3
    assert 1 <= result.position <= 3


def test_cycle_result_to_dict_keys():
    c = Cycler()
    c.load([make_metric("sales", "ok", 500, 10)])
    d = c.current().to_dict()
    assert "pipeline" in d
    assert "status" in d
    assert "position" in d
    assert "total" in d
    assert "total_records" in d
    assert "failed_records" in d


def test_cycler_latest_metric_used_when_multiple():
    import time
    from datetime import timedelta
    now = datetime.now(timezone.utc)
    old = PipelineMetric("pipe", "error", 100, 50, (now - timedelta(hours=2)).isoformat())
    new = PipelineMetric("pipe", "ok", 100, 1, now.isoformat())
    c = Cycler()
    c.load([old, new])
    result = c.current()
    assert result.metric.status == "ok"
