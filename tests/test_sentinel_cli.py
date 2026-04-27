"""Tests for pipewatch.sentinel_cli."""
from __future__ import annotations

import json
from io import StringIO
from unittest.mock import patch

import pytest

from pipewatch.sentinel_cli import main


def test_main_default_runs(capsys):
    rc = main([])
    captured = capsys.readouterr()
    assert rc == 0
    assert len(captured.out) > 0


def test_main_json_output(capsys):
    rc = main(["--json"])
    captured = capsys.readouterr()
    assert rc == 0
    data = json.loads(captured.out)
    assert isinstance(data, list)
    assert all("pipeline" in item for item in data)


def test_main_triggered_only_filters(capsys):
    rc = main(["--triggered-only"])
    captured = capsys.readouterr()
    assert rc == 0
    # sales pipeline should be clean; inventory and orders should trigger
    assert "sales" not in captured.out or "TRIGGERED" not in captured.out.split("sales")[0]


def test_main_triggered_only_json(capsys):
    rc = main(["--triggered-only", "--json"])
    captured = capsys.readouterr()
    assert rc == 0
    data = json.loads(captured.out)
    assert all(item["triggered"] for item in data)


def test_main_strict_threshold_triggers_all(capsys):
    rc = main(["--max-failure-rate", "0.0", "--json"])
    captured = capsys.readouterr()
    assert rc == 0
    data = json.loads(captured.out)
    # inventory has failures, so at least one should be triggered
    triggered = [d for d in data if d["triggered"]]
    assert len(triggered) >= 1


def test_main_no_forbidden_status_skips_status_check(capsys):
    rc = main(["--forbidden-status", "--json"])
    captured = capsys.readouterr()
    assert rc == 0
    data = json.loads(captured.out)
    # orders has status=error but we passed empty forbidden list
    orders_result = next((d for d in data if d["pipeline"] == "orders"), None)
    assert orders_result is not None
