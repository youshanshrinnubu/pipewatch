"""Tests for pipewatch/cycler_cli.py"""
from __future__ import annotations

import json
from unittest.mock import patch

from pipewatch.cycler_cli import main


def test_main_default_runs(capsys):
    ret = main([])
    assert ret == 0
    out = capsys.readouterr().out
    assert "Step" in out


def test_main_json_output(capsys):
    ret = main(["--json", "--steps", "2"])
    assert ret == 0
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert len(data) == 2
    assert "pipeline" in data[0]
    assert "status" in data[0]


def test_main_show_all(capsys):
    ret = main(["--all"])
    assert ret == 0
    out = capsys.readouterr().out
    # Should mention at least one pipeline
    assert "/" in out  # position/total format


def test_main_show_all_json(capsys):
    ret = main(["--all", "--json"])
    assert ret == 0
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list)
    assert len(data) == 4  # four demo pipelines
    pipeline_names = {d["pipeline"] for d in data}
    assert "sales" in pipeline_names
    assert "inventory" in pipeline_names


def test_main_steps_controls_output(capsys):
    ret = main(["--steps", "1"])
    assert ret == 0
    out = capsys.readouterr().out
    lines = [l for l in out.strip().splitlines() if l]
    assert len(lines) == 1
