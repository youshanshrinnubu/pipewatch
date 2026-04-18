"""Tests for pipewatch.deduplicator."""
from __future__ import annotations
import time
from unittest.mock import patch
from pipewatch.alerts import Alert
from pipewatch.deduplicator import Deduplicator, DeduplicatorConfig, _alert_key


def make_alert(pipeline="pipe_a", severity="warning", message="msg") -> Alert:
    return Alert(pipeline=pipeline, severity=severity, message=message)


def test_first_alert_not_duplicate():
    d = Deduplicator()
    a = make_alert()
    assert not d.is_duplicate(a)


def test_duplicate_after_record():
    d = Deduplicator()
    a = make_alert()
    d.record(a)
    assert d.is_duplicate(a)


def test_different_message_not_duplicate():
    d = Deduplicator()
    a1 = make_alert(message="msg1")
    a2 = make_alert(message="msg2")
    d.record(a1)
    assert not d.is_duplicate(a2)


def test_different_severity_not_duplicate():
    d = Deduplicator()
    a1 = make_alert(severity="warning")
    a2 = make_alert(severity="critical")
    d.record(a1)
    assert not d.is_duplicate(a2)


def test_filter_removes_duplicates():
    d = Deduplicator()
    alerts = [make_alert(), make_alert(), make_alert(severity="critical")]
    result = d.filter(alerts)
    assert len(result) == 2


def test_filter_returns_first_occurrence():
    d = Deduplicator()
    a = make_alert(message="first")
    result = d.filter([a, make_alert(message="first"), make_alert(message="second")])
    assert len(result) == 2


def test_eviction_after_window():
    config = DeduplicatorConfig(window_seconds=1.0)
    d = Deduplicator(config)
    a = make_alert()
    d.record(a)
    assert d.is_duplicate(a)
    with patch.object(d, "_now", return_value=time.time() + 2.0):
        assert not d.is_duplicate(a)


def test_stats_returns_counts():
    d = Deduplicator()
    a = make_alert()
    d.record(a)
    d.record(a)
    stats = d.stats()
    key = _alert_key(a)
    assert stats[key] == 2


def test_stats_empty_initially():
    d = Deduplicator()
    assert d.stats() == {}


def test_alert_key_stable():
    a = make_alert()
    assert _alert_key(a) == _alert_key(a)
