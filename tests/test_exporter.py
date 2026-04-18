"""Tests for pipewatch.exporter."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime

import pytest

from pipewatch.exporter import (
    export_metrics_csv,
    export_metrics_json,
    export_summary_json,
    write_export,
)
from pipewatch.metrics import PipelineMetric


def make_metric(name: str = "pipe", status: str = "ok", failures: int = 0, total: int = 10) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        status=status,
        records_processed=total,
        records_failed=failures,
        duration_seconds=1.5,
    )


def test_export_metrics_json_empty():
    result = export_metrics_json([])
    assert result == "[]"


def test_export_metrics_json_contains_pipeline_name():
    metrics = [make_metric("alpha")]
    data = json.loads(export_metrics_json(metrics))
    assert data[0]["pipeline_name"] == "alpha"


def test_export_metrics_json_multiple():
    metrics = [make_metric("a"), make_metric("b")]
    data = json.loads(export_metrics_json(metrics))
    assert len(data) == 2


def test_export_metrics_csv_empty():
    assert export_metrics_csv([]) == ""


def test_export_metrics_csv_has_header():
    metrics = [make_metric("pipe")]
    csv_str = export_metrics_csv(metrics)
    assert "pipeline_name" in csv_str


def test_export_metrics_csv_has_data_row():
    metrics = [make_metric("mypipe")]
    csv_str = export_metrics_csv(metrics)
    assert "mypipe" in csv_str


def test_export_summary_json_groups_by_pipeline():
    metrics = [make_metric("p1"), make_metric("p1"), make_metric("p2")]
    data = json.loads(export_summary_json(metrics))
    names = {row["pipeline"] for row in data}
    assert names == {"p1", "p2"}


def test_export_summary_json_total_runs():
    metrics = [make_metric("p1"), make_metric("p1")]
    data = json.loads(export_summary_json(metrics))
    assert data[0]["total_runs"] == 2


def test_write_export_creates_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "out.json")
        write_export('{"ok": true}', path)
        assert os.path.exists(path)
        with open(path) as f:
            assert json.load(f)["ok"] is True
