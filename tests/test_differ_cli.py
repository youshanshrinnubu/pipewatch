import json
import pytest
from unittest.mock import patch, MagicMock
from pipewatch.differ_cli import main
from pipewatch.metrics import PipelineMetric
from pipewatch.differ import MetricDiff


def make_metric(pipeline="pipe", total=100, failed=5, status="ok"):
    return PipelineMetric(
        pipeline=pipeline,
        total_records=total,
        failed_records=failed,
        status=status,
        duration_seconds=1.0,
        timestamp="2024-01-01T00:00:00",
    )


@patch("pipewatch.differ_cli.SnapshotStore")
def test_main_no_diffs_prints_message(mock_store, capsys):
    instance = MagicMock()
    instance.load_latest.return_value = []
    mock_store.return_value = instance
    rc = main(["prev.json", "curr.json"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "No differences" in out


@patch("pipewatch.differ_cli.SnapshotStore")
def test_main_json_output(mock_store, capsys):
    prev = make_metric("pipe_a", failed=5)
    curr = make_metric("pipe_a", failed=20)
    instance = MagicMock()
    instance.load_latest.side_effect = [[prev], [curr]]
    mock_store.return_value = instance
    rc = main(["prev.json", "curr.json", "--json"])
    assert rc == 0
    out = capsys.readouterr().out
    data = json.loads(out)
    assert len(data) == 1
    assert data[0]["pipeline"] == "pipe_a"
    assert data[0]["degraded"] is True


@patch("pipewatch.differ_cli.SnapshotStore")
def test_main_degraded_only_filter(mock_store, capsys):
    prev = [make_metric("good", failed=5), make_metric("bad", failed=5)]
    curr = [make_metric("good", failed=5), make_metric("bad", failed=50)]
    instance = MagicMock()
    instance.load_latest.side_effect = [prev, curr]
    mock_store.return_value = instance
    rc = main(["prev.json", "curr.json", "--degraded-only"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "bad" in out
    assert "good" not in out
