"""Tests for built-in alert handlers."""
import io
import json
import os
import tempfile
from datetime import datetime
from pipewatch.alerts import Alert
from pipewatch.handlers import stdout_handler, stderr_handler, json_handler, make_file_handler


def make_alert(severity="warning"):
    return Alert(
        pipeline_name="test_pipe",
        reason="high failure rate",
        severity=severity,
        triggered_at=datetime(2024, 1, 15, 10, 30, 0),
    )


def test_json_handler_writes_valid_json():
    buf = io.StringIO()
    handler = json_handler(stream=buf)
    handler(make_alert())
    buf.seek(0)
    data = json.loads(buf.read())
    assert data["pipeline_name"] == "test_pipe"
    assert data["severity"] == "warning"


def test_json_handler_critical():
    buf = io.StringIO()
    handler = json_handler(stream=buf)
    handler(make_alert(severity="critical"))
    buf.seek(0)
    data = json.loads(buf.read())
    assert data["severity"] == "critical"


def test_file_handler_appends(tmp_path):
    path = str(tmp_path / "alerts.jsonl")
    handler = make_file_handler(path)
    handler(make_alert())
    handler(make_alert(severity="critical"))
    lines = open(path).readlines()
    assert len(lines) == 2
    assert json.loads(lines[1])["severity"] == "critical"


def test_stdout_handler_runs(capsys):
    stdout_handler(make_alert())
    captured = capsys.readouterr()
    assert "WARNING" in captured.out
    assert "test_pipe" in captured.out


def test_stderr_handler_runs(capsys):
    stderr_handler(make_alert(severity="critical"))
    captured = capsys.readouterr()
    assert "CRITICAL" in captured.err
