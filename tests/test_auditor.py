"""Tests for pipewatch.auditor."""
from __future__ import annotations

import json
import os
import time

import pytest

from pipewatch.auditor import AuditLog, AuditEntry
from pipewatch.metrics import PipelineMetric
from pipewatch.alerts import Alert


def make_metric(pipeline="pipe1", status="ok", total=100, failed=2) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=pipeline,
        status=status,
        total_records=total,
        failed_records=failed,
        timestamp=time.time(),
    )


def make_alert(pipeline="pipe1", severity="warning", message="bad") -> Alert:
    return Alert(pipeline_name=pipeline, severity=severity, message=message)


def test_record_metric_creates_entry():
    log = AuditLog()
    m = make_metric()
    entry = log.record_metric(m)
    assert entry.event_type == "metric"
    assert entry.pipeline == "pipe1"
    assert entry.detail["status"] == "ok"


def test_record_alert_creates_entry():
    log = AuditLog()
    a = make_alert(severity="critical")
    entry = log.record_alert(a)
    assert entry.event_type == "alert"
    assert entry.detail["severity"] == "critical"


def test_entries_returns_all_by_default():
    log = AuditLog()
    log.record_metric(make_metric("a"))
    log.record_alert(make_alert("b"))
    assert len(log.entries()) == 2


def test_filter_by_pipeline():
    log = AuditLog()
    log.record_metric(make_metric("alpha"))
    log.record_metric(make_metric("beta"))
    result = log.entries(pipeline="alpha")
    assert len(result) == 1
    assert result[0].pipeline == "alpha"


def test_filter_by_event_type():
    log = AuditLog()
    log.record_metric(make_metric())
    log.record_alert(make_alert())
    alerts = log.entries(event_type="alert")
    assert len(alerts) == 1
    assert alerts[0].event_type == "alert"


def test_ids_are_sequential():
    log = AuditLog()
    e1 = log.record_metric(make_metric())
    e2 = log.record_alert(make_alert())
    assert e2.id == e1.id + 1


def test_to_dict_contains_required_keys():
    log = AuditLog()
    entry = log.record_metric(make_metric())
    d = entry.to_dict()
    for key in ("id", "event_type", "pipeline", "timestamp", "detail"):
        assert key in d


def test_persist_and_reload(tmp_path):
    path = str(tmp_path / "audit.jsonl")
    log = AuditLog(path=path)
    log.record_metric(make_metric("pipe1"))
    log.record_alert(make_alert("pipe2"))

    log2 = AuditLog(path=path)
    entries = log2.entries()
    assert len(entries) == 2
    assert entries[0].pipeline == "pipe1"
    assert entries[1].pipeline == "pipe2"


def test_reload_empty_when_no_file(tmp_path):
    path = str(tmp_path / "missing.jsonl")
    log = AuditLog(path=path)
    assert log.entries() == []
