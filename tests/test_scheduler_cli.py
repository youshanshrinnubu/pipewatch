"""Tests for pipewatch.scheduler_cli."""

import json
from io import StringIO
import pytest
from unittest.mock import patch
from pipewatch.scheduler_cli import main


def test_main_json_output(capsys):
    main(["--jobs", "pipe_a", "--interval", "0.1", "--duration", "0.35", "--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert "pipe_a" in data
    assert data["pipe_a"] >= 1


def test_main_text_output(capsys):
    main(["--jobs", "pipe_x", "--interval", "0.1", "--duration", "0.25"])
    out = capsys.readouterr().out
    assert "pipe_x" in out


def test_main_multiple_jobs(capsys):
    main(["--jobs", "a", "b", "--interval", "0.1", "--duration", "0.25", "--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert "a" in data
    assert "b" in data


def test_main_zero_duration(capsys):
    main(["--jobs", "noop", "--interval", "1.0", "--duration", "0.0", "--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["noop"] == 0
