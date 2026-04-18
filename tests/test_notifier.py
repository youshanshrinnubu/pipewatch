"""Tests for pipewatch.notifier."""
import time
from unittest.mock import MagicMock

import pytest

from pipewatch.alerts import Alert
from pipewatch.notifier import Notifier, NotifierConfig


def make_alert(pipeline="pipe1", severity="warning", message="fail"):
    return Alert(pipeline=pipeline, severity=severity, message=message)


def test_should_send_first_time():
    n = Notifier()
    assert n.should_send(make_alert()) is True


def test_notify_calls_handler():
    n = Notifier()
    handler = MagicMock()
    alert = make_alert()
    result = n.notify(alert, handler)
    assert result is True
    handler.assert_called_once_with(alert)


def test_notify_suppressed_after_max_repeats():
    cfg = NotifierConfig(cooldown_seconds=60, max_repeats=2)
    n = Notifier(cfg)
    handler = MagicMock()
    alert = make_alert()
    n.notify(alert, handler)
    n.notify(alert, handler)
    n.notify(alert, handler)
    suppressed = n.notify(alert, handler)
    assert suppressed is False
    assert handler.call_count == 3


def test_notify_resets_after_cooldown(monkeypatch):
    cfg = NotifierConfig(cooldown_seconds=1, max_repeats=0)
    n = Notifier(cfg)
    handler = MagicMock()
    alert = make_alert()
    n.notify(alert, handler)
    monkeypatch.setattr(time, "time", lambda: time.time() + 2)
    result = n.notify(alert, handler)
    assert result is True


def test_different_pipelines_tracked_separately():
    n = Notifier(NotifierConfig(cooldown_seconds=60, max_repeats=0))
    handler = MagicMock()
    a1 = make_alert(pipeline="p1")
    a2 = make_alert(pipeline="p2")
    n.notify(a1, handler)
    assert n.notify(a1, handler) is False
    assert n.notify(a2, handler) is True


def test_reset_clears_state():
    n = Notifier(NotifierConfig(cooldown_seconds=60, max_repeats=0))
    handler = MagicMock()
    alert = make_alert()
    n.notify(alert, handler)
    assert n.notify(alert, handler) is False
    n.reset()
    assert n.notify(alert, handler) is True


def test_reset_specific_pipeline():
    n = Notifier(NotifierConfig(cooldown_seconds=60, max_repeats=0))
    handler = MagicMock()
    a1 = make_alert(pipeline="p1")
    a2 = make_alert(pipeline="p2")
    n.notify(a1, handler)
    n.notify(a2, handler)
    n.reset(pipeline="p1")
    assert n.notify(a1, handler) is True
    assert n.notify(a2, handler) is False
