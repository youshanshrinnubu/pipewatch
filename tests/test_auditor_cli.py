"""Tests for pipewatch.auditor_cli."""
from __future__ import annotations

import json
import time

import pytest

from pipewatch.auditor import AuditLog
from pipewatch.auditor_cli import main
from pipewatch.metrics import PipelineMetric
from pipewatch.alerts import Alert


def _make_log(tmp_path):
    path = str(tmp_path / "audit.jsonl")
    log = AuditLog(path=path)
    log.record_metric(
        PipelineMetric(
            pipeline_name="sales",
            status="ok",
            total_records=200,
            failed_records=1,
            timestamp=time.time(),
        )
    )
    log.record_alert(Alert(pipeline_name="inventory", severity="critical", message="down"))
    return path


def test_main_text_output(tmp_path, capsys):
    path = _make_log(tmp_path)
    rc = main([path])
    out = capsys.readouterr().out
    assert rc == 0
    assert "METRIC" in out
    assert "ALERT" in out


def test_main_json_output(tmp_path, capsys):
    path = _make_log(tmp_path)
    rc = main([path, "--json"])
    out = capsys.readouterr().out
    assert rc == 0
    data = json.loads(out)
    assert isinstance(data, list)
    assert len(data) == 2


def test_main_filter_pipeline(tmp_path, capsys):
    path = _make_log(tmp_path)
    rc = main([path, "--pipeline", "sales", "--json"])
    data = json.loads(capsys.readouterr().out)
    assert all(e["pipeline"] == "sales" for e in data)


def test_main_filter_event_type(tmp_path, capsys):
    path = _make_log(tmp_path)
    rc = main([path, "--type", "alert", "--json"])
    data = json.loads(capsys.readouterr().out)
    assert all(e["event_type"] == "alert" for e in data)


def test_main_last_n(tmp_path, capsys):
    path = _make_log(tmp_path)
    rc = main([path, "--last", "1", "--json"])
    data = json.loads(capsys.readouterr().out)
    assert len(data) == 1


def test_main_empty_log_message(tmp_path, capsys):
    path = str(tmp_path / "empty.jsonl")
    open(path, "w").close()
    rc = main([path])
    out = capsys.readouterr().out
    assert "No audit entries" in out
