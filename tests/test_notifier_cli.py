"""Tests for pipewatch.notifier_cli."""
import json
from io import StringIO
from unittest.mock import patch

import pytest

from pipewatch.notifier_cli import main


def test_main_default_runs(capsys):
    rc = main(["--count", "3", "--max-repeats", "3"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "sent" in out or "Summary" in out


def test_main_summary_text(capsys):
    rc = main(["--count", "5", "--max-repeats", "2", "--format", "text"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Summary" in out
    assert "suppressed" in out


def test_main_json_output(capsys):
    rc = main(["--count", "4", "--max-repeats", "1", "--format", "json"])
    assert rc == 0
    lines = [l for l in capsys.readouterr().out.strip().splitlines() if l.startswith("{")]
    summary = json.loads(lines[-1])
    assert summary["total"] == 4
    assert summary["sent"] + summary["suppressed"] == 4


def test_main_all_sent_when_max_repeats_high(capsys):
    rc = main(["--count", "3", "--max-repeats", "10", "--format", "json"])
    assert rc == 0
    lines = [l for l in capsys.readouterr().out.strip().splitlines() if l.startswith("{")]
    summary = json.loads(lines[-1])
    assert summary["sent"] == 3
    assert summary["suppressed"] == 0


def test_main_severity_critical(capsys):
    rc = main(["--severity", "critical", "--count", "2", "--max-repeats", "5"])
    assert rc == 0
