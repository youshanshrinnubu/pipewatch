"""Tests for pipewatch.throttle."""
from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from pipewatch.alerts import Alert
from pipewatch.metrics import PipelineMetric
from pipewatch.throttle import Throttle, ThrottleConfig


def make_alert(pipeline="pipe1", severity="warning") -> Alert:
    metric = PipelineMetric(pipeline=pipeline, status="ok", records=100, failures=10)
    return Alert(pipeline=pipeline, severity=severity, message="test", metric=metric)


def test_allow_first_alert():
    t = Throttle(ThrottleConfig(max_per_window=3))
    assert t.allow(make_alert()) is True


def test_allow_up_to_max():
    t = Throttle(ThrottleConfig(max_per_window=3))
    a = make_alert()
    assert t.allow(a) is True
    assert t.allow(a) is True
    assert t.allow(a) is True


def test_block_after_max():
    t = Throttle(ThrottleConfig(max_per_window=2))
    a = make_alert()
    t.allow(a)
    t.allow(a)
    assert t.allow(a) is False


def test_different_pipelines_tracked_independently():
    t = Throttle(ThrottleConfig(max_per_window=1))
    a1 = make_alert(pipeline="p1")
    a2 = make_alert(pipeline="p2")
    assert t.allow(a1) is True
    assert t.allow(a2) is True
    assert t.allow(a1) is False


def test_different_severities_tracked_independently():
    t = Throttle(ThrottleConfig(max_per_window=1))
    a_warn = make_alert(severity="warning")
    a_crit = make_alert(severity="critical")
    assert t.allow(a_warn) is True
    assert t.allow(a_crit) is True
    assert t.allow(a_warn) is False


def test_reset_clears_bucket():
    t = Throttle(ThrottleConfig(max_per_window=1))
    a = make_alert()
    t.allow(a)
    assert t.allow(a) is False
    t.reset(a)
    assert t.allow(a) is True


def test_window_expiry_allows_again():
    t = Throttle(ThrottleConfig(window_seconds=1.0, max_per_window=1))
    a = make_alert()
    t.allow(a)
    assert t.allow(a) is False
    # Simulate time passing beyond the window
    key = f"{a.pipeline}:{a.severity}"
    t._buckets[key].window_start -= 2.0
    assert t.allow(a) is True


def test_status_returns_correct_remaining():
    t = Throttle(ThrottleConfig(max_per_window=3))
    a = make_alert()
    t.allow(a)
    t.allow(a)
    st = t.status(a)
    assert st["count"] == 2
    assert st["remaining"] == 1


def test_status_before_any_alert():
    t = Throttle(ThrottleConfig(max_per_window=5))
    st = t.status(make_alert())
    assert st["count"] == 0
    assert st["remaining"] == 5
