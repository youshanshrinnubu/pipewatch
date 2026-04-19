"""Tests for pipewatch.pipeline_health_cli."""
import json
import pytest
from unittest.mock import patch
from pipewatch.metrics import PipelineMetric
from pipewatch.pipeline_health_cli import main


def make_metric(pipeline="pipe", status="ok", records=100, failures=5, duration=1.0):
    return PipelineMetric(
        pipeline=pipeline,
        status=status,
        records_processed=records,
        records_failed=failures,
        duration_seconds=duration,
    )


def _patch_store(metrics):
    return patch("pipewatch.pipeline_health_cli.SnapshotStore", **{
        "return_value.load.return_value": metrics
    })


def test_main_text_output(capsys):
    with _patch_store([make_metric()]):
        main(["snap.json"])
    out = capsys.readouterr().out
    assert "pipe" in out


def test_main_json_output(capsys):
    with _patch_store([make_metric()]):
        main(["snap.json", "--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe"


def test_main_pipeline_filter(capsys):
    metrics = [
        make_metric(pipeline="alpha"),
        make_metric(pipeline="beta"),
    ]
    with _patch_store(metrics):
        main(["snap.json", "--pipeline", "alpha"])
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" not in out


def test_main_status_filter_critical(capsys):
    metrics = [
        make_metric(pipeline="good", failures=2),
        make_metric(pipeline="bad", failures=80),
    ]
    with _patch_store(metrics):
        main(["snap.json", "--status", "critical"])
    out = capsys.readouterr().out
    assert "bad" in out
    assert "good" not in out


def test_main_no_metrics_exits(capsys):
    with _patch_store([]):
        with pytest.raises(SystemExit):
            main(["snap.json"])


def test_main_no_matching_pipelines(capsys):
    with _patch_store([make_metric(pipeline="pipe")]):
        main(["snap.json", "--pipeline", "nonexistent"])
    out = capsys.readouterr().out
    assert "No matching" in out
