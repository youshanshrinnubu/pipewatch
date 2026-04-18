"""Tests for pipewatch.replay_cli."""
import json
import os
import tempfile
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.snapshot import SnapshotStore
from pipewatch.replay_cli import main


def make_metric(pipeline="pipe", status="ok", total=100, failed=0):
    return PipelineMetric(
        pipeline_name=pipeline,
        status=status,
        total_records=total,
        failed_records=failed,
        duration_seconds=1.0,
    )


@pytest.fixture
def snap_file():
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        path = f.name
    store = SnapshotStore(path)
    store.save(make_metric(pipeline="alpha", failed=0))
    store.save(make_metric(pipeline="alpha", failed=25))
    store.save(make_metric(pipeline="beta", failed=0))
    yield path
    os.unlink(path)


def test_main_text_output(snap_file, capsys):
    rc = main([snap_file])
    assert rc == 0
    out = capsys.readouterr().out
    assert "alpha" in out or "beta" in out


def test_main_json_output(snap_file, capsys):
    rc = main([snap_file, "--json"])
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    assert len(data) == 3


def test_main_summary_text(snap_file, capsys):
    rc = main([snap_file, "--summary"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "total_events" in out


def test_main_summary_json(snap_file, capsys):
    rc = main([snap_file, "--summary", "--json"])
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    assert data["total_events"] == 3


def test_main_pipeline_filter(snap_file, capsys):
    rc = main([snap_file, "--pipeline", "beta", "--json"])
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    assert all(e["pipeline"] == "beta" for e in data)


def test_main_missing_file_returns_error(capsys):
    rc = main(["/nonexistent/path.json"])
    assert rc == 1
