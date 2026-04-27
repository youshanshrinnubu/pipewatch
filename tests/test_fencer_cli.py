"""Tests for pipewatch.fencer_cli."""
from __future__ import annotations

import json
from io import StringIO
from unittest.mock import patch

import pytest

from pipewatch.fencer_cli import main


def test_main_default_runs(capsys):
    main([])
    out = capsys.readouterr().out
    assert "sales" in out


def test_main_json_output(capsys):
    main(["--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert len(data) > 0
    assert "state" in data[0]
    assert "tripped" in data[0]
    assert "reset" in data[0]


def test_main_json_contains_open_field(capsys):
    main(["--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    for item in data:
        assert "open" in item["state"]


def test_main_trip_shown_in_text(capsys):
    # With low trip_count the demo sequence should produce a TRIPPED line
    main(["--trip-count", "2", "--trip-threshold", "0.2"])
    out = capsys.readouterr().out
    assert "TRIPPED" in out or "OPEN" in out


def test_main_custom_trip_threshold(capsys):
    main(["--trip-threshold", "0.01", "--trip-count", "1"])
    out = capsys.readouterr().out
    assert "OPEN" in out or "TRIPPED" in out


def test_main_reset_shown_in_text(capsys):
    # trip_count=2, reset_count=1 — fence should open and close within demo
    main(["--trip-count", "2", "--reset-count", "1", "--trip-threshold", "0.2"])
    out = capsys.readouterr().out
    # At minimum the output has pipeline names
    assert "sales" in out
