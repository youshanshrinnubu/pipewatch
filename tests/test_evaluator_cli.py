"""Tests for pipewatch.evaluator_cli."""
from __future__ import annotations

import json
import os
import tempfile
from typing import List
from unittest.mock import patch

import pytest

from pipewatch.evaluator_cli import main
from pipewatch.metrics import PipelineMetric
from pipewatch.snapshot import SnapshotStore


def make_metric(
    pipeline: str = "pipe",
    total: int = 100,
    failed: int = 0,
    status: str = "ok",
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        total_records=total,
        failed_records=failed,
        status=status,
    )


@pytest.fixture()
def snap_file(tmp_path):
    metrics = [
        make_metric(pipeline="alpha", failed=0),
        make_metric(pipeline="beta", failed=15, total=100),
        make_metric(pipeline="gamma", status="error"),
    ]
    path = str(tmp_path / "snap.json")
    store = SnapshotStore(path)
    for m in metrics:
        store.save(m)
    return path


def test_main_text_output(snap_file, capsys):
    rc = main(["--store", snap_file])
    assert rc == 0
    captured = capsys.readouterr()
    assert "alpha" in captured.out
    assert "beta" in captured.out
    assert "gamma" in captured.out


def test_main_json_output(snap_file, capsys):
    rc = main(["--store", snap_file, "--json"])
    assert rc == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert isinstance(data, list)
    assert len(data) == 3
    keys = set(data[0].keys())
    assert keys == {"pipeline", "score", "tier", "reasons"}


def test_main_tier_filter_critical(snap_file, capsys):
    rc = main(["--store", snap_file, "--tier", "critical"])
    assert rc == 0
    captured = capsys.readouterr()
    assert "gamma" in captured.out
    assert "alpha" not in captured.out


def test_main_pipeline_filter(snap_file, capsys):
    rc = main(["--store", snap_file, "--pipeline", "beta"])
    assert rc == 0
    captured = capsys.readouterr()
    assert "beta" in captured.out
    assert "alpha" not in captured.out


def test_main_no_results_message(snap_file, capsys):
    rc = main(["--store", snap_file, "--pipeline", "nonexistent"])
    assert rc == 0
    captured = capsys.readouterr()
    assert "No evaluation results" in captured.out


def test_main_custom_thresholds(snap_file, capsys):
    # With very tight thresholds even 0-failure pipelines may be flagged
    rc = main(["--store", snap_file, "--warning-fr", "0.0", "--json"])
    assert rc == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert len(data) == 3
