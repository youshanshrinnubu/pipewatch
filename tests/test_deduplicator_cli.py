"""Tests for pipewatch.deduplicator_cli."""
from __future__ import annotations
import json
from io import StringIO
from unittest.mock import patch
from pipewatch.deduplicator_cli import main


def test_main_text_output(capsys):
    main([])
    out = capsys.readouterr().out
    assert "Total alerts" in out
    assert "Passed" in out
    assert "Suppressed" in out


def test_main_json_output(capsys):
    main(["--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert "total" in data
    assert "passed" in data
    assert "suppressed" in data
    assert data["suppressed"] >= 0


def test_main_suppressed_count(capsys):
    main(["--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["total"] == 5
    assert data["suppressed"] == 2


def test_main_custom_window(capsys):
    main(["--window", "0.0", "--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    # With zero window all evicted immediately, none suppressed
    assert data["suppressed"] == 0
