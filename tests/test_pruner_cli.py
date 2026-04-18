"""Tests for pipewatch.pruner_cli."""
from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.pruner_cli import main
from pipewatch.snapshot import SnapshotStore


def make_metric(pipeline="pipe", offset_seconds=0):
    return PipelineMetric(
        pipeline=pipeline,
        timestamp=datetime.utcnow() - timedelta(seconds=offset_seconds),
        total_records=100,
        failed_records=0,
        status="ok",
    )


def test_main_requires_constraint(capsys):
    rc = main(["--store", "nonexistent.json"])
    assert rc == 1
    captured = capsys.readouterr()
    assert "specify" in captured.err


def test_main_max_count_text(tmp_path, capsys):
    store_path = str(tmp_path / "snap.json")
    store = SnapshotStore(path=store_path)
    for i in range(4):
        store.save(make_metric(pipeline="mypipe", offset_seconds=i * 10))
    rc = main(["--store", store_path, "--max-count", "2"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "mypipe" in out
    assert "removed=2" in out


def test_main_max_count_json(tmp_path, capsys):
    store_path = str(tmp_path / "snap.json")
    store = SnapshotStore(path=store_path)
    for i in range(3):
        store.save(make_metric(pipeline="p", offset_seconds=i * 5))
    rc = main(["--store", store_path, "--max-count", "1", "--json"])
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "p"
    assert data[0]["removed"] == 2


def test_main_empty_store_no_crash(tmp_path, capsys):
    store_path = str(tmp_path / "empty.json")
    rc = main(["--store", store_path, "--max-count", "5"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "No pipelines" in out
