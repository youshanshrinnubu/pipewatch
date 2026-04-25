"""Tests for pipewatch.cursor."""
from __future__ import annotations

import json
import os
import time

import pytest

from pipewatch.cursor import (
    CursorState,
    CursorStore,
    advance_all,
    new_metrics,
)
from pipewatch.metrics import PipelineMetric


def make_metric(pipeline: str, ts: float, failed: int = 0, total: int = 100) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=pipeline,
        status="ok",
        total_records=total,
        failed_records=failed,
        timestamp=ts,
    )


# ---------------------------------------------------------------------------
# CursorState unit tests
# ---------------------------------------------------------------------------

def test_get_returns_none_for_unknown():
    state = CursorState()
    assert state.get("pipe_a") is None


def test_advance_sets_position():
    state = CursorState()
    state.advance("pipe_a", 100.0)
    assert state.get("pipe_a") == 100.0


def test_advance_does_not_go_backwards():
    state = CursorState(positions={"pipe_a": 200.0})
    state.advance("pipe_a", 100.0)
    assert state.get("pipe_a") == 200.0


def test_advance_moves_forward():
    state = CursorState(positions={"pipe_a": 100.0})
    state.advance("pipe_a", 200.0)
    assert state.get("pipe_a") == 200.0


def test_reset_single_pipeline():
    state = CursorState(positions={"pipe_a": 100.0, "pipe_b": 200.0})
    state.reset("pipe_a")
    assert state.get("pipe_a") is None
    assert state.get("pipe_b") == 200.0


def test_reset_all():
    state = CursorState(positions={"pipe_a": 100.0, "pipe_b": 200.0})
    state.reset()
    assert state.positions == {}


# ---------------------------------------------------------------------------
# CursorStore persistence
# ---------------------------------------------------------------------------

def test_save_and_load_roundtrip(tmp_path):
    path = str(tmp_path / "cursor.json")
    store = CursorStore(path)
    state = CursorState(positions={"alpha": 1_000.0, "beta": 2_000.0})
    store.save(state)
    loaded = store.load()
    assert loaded.get("alpha") == 1_000.0
    assert loaded.get("beta") == 2_000.0


def test_load_returns_empty_when_no_file(tmp_path):
    store = CursorStore(str(tmp_path / "missing.json"))
    state = store.load()
    assert state.positions == {}


# ---------------------------------------------------------------------------
# new_metrics / advance_all helpers
# ---------------------------------------------------------------------------

def test_new_metrics_all_new_when_no_cursor():
    state = CursorState()
    metrics = [make_metric("p", 1.0), make_metric("p", 2.0)]
    result = new_metrics(metrics, state)
    assert len(result) == 2


def test_new_metrics_filters_old():
    state = CursorState(positions={"p": 5.0})
    metrics = [make_metric("p", 3.0), make_metric("p", 7.0)]
    result = new_metrics(metrics, state)
    assert len(result) == 1
    assert result[0].timestamp == 7.0


def test_new_metrics_excludes_equal_timestamp():
    state = CursorState(positions={"p": 5.0})
    metrics = [make_metric("p", 5.0)]
    result = new_metrics(metrics, state)
    assert result == []


def test_advance_all_updates_state():
    state = CursorState()
    metrics = [make_metric("p", 1.0), make_metric("p", 3.0), make_metric("q", 2.0)]
    advance_all(metrics, state)
    assert state.get("p") == 3.0
    assert state.get("q") == 2.0
