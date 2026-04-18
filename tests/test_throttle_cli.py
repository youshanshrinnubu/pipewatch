"""Tests for pipewatch.throttle_cli."""
from __future__ import annotations

import json
from io import StringIO
from unittest.mock import patch

from pipewatch.throttle_cli import main


def test_main_text_output(capsys):
    main(["--count", "3", "--max", "5"])
    out = capsys.readouterr().out
    assert "ALLOW" in out
    assert "attempt=1" in out


def test_main_json_output(capsys):
    main(["--count", "4", "--max", "2", "--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert len(data) == 4
    assert data[0]["allowed"] is True
    assert data[2]["allowed"] is False


def test_main_all_blocked_after_max(capsys):
    main(["--count", "5", "--max", "1", "--json"])
    data = json.loads(capsys.readouterr().out)
    allowed = [r["allowed"] for r in data]
    assert allowed[0] is True
    assert all(not a for a in allowed[1:])


def test_main_custom_window(capsys):
    main(["--count", "2", "--max", "2", "--window", "120", "--json"])
    data = json.loads(capsys.readouterr().out)
    assert all(r["allowed"] for r in data)
