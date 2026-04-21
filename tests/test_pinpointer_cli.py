"""Tests for pipewatch.pinpointer_cli."""
from __future__ import annotations

import json
import tempfile
import os

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.snapshot import SnapshotStore
from pipewatch.pinpointer_cli import main


def make_metric(
    pipeline="pipe",
    status="ok",
    total=100,
    failed=0,
    duration=1.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        status=status,
        total_records=total,
        failed_records=failed,
        duration_seconds=duration,
    )


@pytest.fixture()
def snap_file(tmp_path):
    path = str(tmp_path / "snap.json")
    store = SnapshotStore(path)
    store.save(make_metric(pipeline="alpha", status="ok", total=100, failed=2))
    store.save(make_metric(pipeline="beta", status="error", total=100, failed=80))
    return path


def test_main_text_output(snap_file, capsys):
    rc = main([snap_file])
    assert rc == 0
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" in out


def test_main_json_output(snap_file, capsys):
    rc = main([snap_file, "--json"])
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    assert len(data) == 2
    pipelines = {d["pipeline"] for d in data}
    assert "alpha" in pipelines
    assert "beta" in pipelines


def test_main_pipeline_filter(snap_file, capsys):
    rc = main([snap_file, "--pipeline", "alpha"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" not in out


def test_main_min_score_filter(snap_file, capsys):
    rc = main([snap_file, "--json", "--min-score", "0.5"])
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    for item in data:
        assert item["score"] <= 0.5


def test_main_empty_snapshot_returns_error(tmp_path, capsys):
    path = str(tmp_path / "empty.json")
    rc = main([path])
    assert rc == 1
