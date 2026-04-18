"""Tests for pipewatch.watchdog_cli."""

from unittest.mock import patch
from datetime import datetime, timezone, timedelta
import json

from pipewatch.metrics import PipelineMetric
from pipewatch.watchdog_cli import main


def _make_metrics(ages):
    now = datetime.now(timezone.utc)
    return [
        PipelineMetric(f"pipe_{i}", "ok", 100, 1, now - timedelta(seconds=age))
        for i, age in enumerate(ages)
    ]


def test_main_default_runs(capsys):
    assert main([]) == 0
    out = capsys.readouterr().out
    assert len(out) > 0


def test_main_json_output(capsys):
    assert main(["--json"]) == 0
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert all("pipeline" in d for d in data)


def test_main_stale_only(capsys):
    assert main(["--stale-only"]) == 0
    out = capsys.readouterr().out
    assert "[OK]" not in out


def test_main_no_stale_message(capsys):
    now = datetime.now(timezone.utc)
    fresh = [PipelineMetric("p", "ok", 100, 0, now - timedelta(seconds=5))]
    with patch("pipewatch.watchdog_cli._make_demo_metrics", return_value=fresh):
        rc = main(["--stale-only"])
    assert rc == 0
    assert "No stale" in capsys.readouterr().out
