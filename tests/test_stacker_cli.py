"""Tests for pipewatch.stacker_cli."""
from __future__ import annotations

import json

from pipewatch.stacker_cli import main


def test_main_default_runs(capsys):
    rc = main([])
    assert rc == 0
    out = capsys.readouterr().out
    assert "sales" in out
    assert "inventory" in out


def test_main_json_output(capsys):
    rc = main(["--json"])
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    pipelines = {entry["pipeline"] for entry in data}
    assert "sales" in pipelines
    assert "inventory" in pipelines


def test_main_json_has_metrics_list(capsys):
    main(["--json"])
    data = json.loads(capsys.readouterr().out)
    for entry in data:
        assert "metrics" in entry
        assert isinstance(entry["metrics"], list)


def test_main_capacity_limits_stack(capsys):
    main(["--json", "--capacity", "1"])
    data = json.loads(capsys.readouterr().out)
    for entry in data:
        assert entry["size"] <= 1
        assert entry["capacity"] == 1


def test_main_text_shows_status(capsys):
    main([])
    out = capsys.readouterr().out
    # At least one of the known statuses should appear
    assert any(s in out for s in ("ok", "warning", "error"))
