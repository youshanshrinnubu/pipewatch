"""Tests for pipewatch.escalator."""
from unittest.mock import patch
from time import time

from pipewatch.alerts import Alert
from pipewatch.escalator import Escalator, EscalationConfig
from pipewatch.metrics import PipelineMetric


def make_metric(**kw) -> PipelineMetric:
    defaults = dict(pipeline="pipe", status="ok", total_records=100,
                    failed_records=5, duration_seconds=1.0)
    defaults.update(kw)
    return PipelineMetric(**defaults)


def make_alert(severity="warning", pipeline="pipe") -> Alert:
    return Alert(pipeline=pipeline, severity=severity,
                 message="test", metric=make_metric(pipeline=pipeline))


def test_first_warning_not_escalated():
    e = Escalator(EscalationConfig(warn_to_critical_after=3))
    result = e.process(make_alert())
    assert result.severity == "warning"


def test_escalates_after_threshold():
    e = Escalator(EscalationConfig(warn_to_critical_after=3))
    for _ in range(2):
        e.process(make_alert())
    result = e.process(make_alert())
    assert result.severity == "critical"
    assert "ESCALATED" in result.message


def test_critical_alert_unchanged():
    e = Escalator(EscalationConfig(warn_to_critical_after=1))
    alert = make_alert(severity="critical")
    result = e.process(alert)
    assert result.severity == "critical"
    assert "ESCALATED" not in result.message


def test_different_pipelines_tracked_independently():
    e = Escalator(EscalationConfig(warn_to_critical_after=2))
    e.process(make_alert(pipeline="a"))
    result_b = e.process(make_alert(pipeline="b"))
    assert result_b.severity == "warning"  # b has only 1 occurrence


def test_window_prunes_old_entries():
    e = Escalator(EscalationConfig(warn_to_critical_after=2, window_seconds=10))
    old_time = time() - 20
    alert = make_alert()
    key = f"{alert.pipeline}:{alert.severity}"
    from pipewatch.escalator import _Record
    e._records[key] = _Record(timestamps=[old_time, old_time])
    result = e.process(alert)
    # old timestamps pruned; only 1 new one — should NOT escalate
    assert result.severity == "warning"


def test_process_all_returns_list():
    e = Escalator()
    alerts = [make_alert(), make_alert(pipeline="other")]
    results = e.process_all(alerts)
    assert len(results) == 2


def test_repeat_count_zero_before_any():
    e = Escalator()
    assert e.repeat_count(make_alert()) == 0


def test_repeat_count_increments():
    e = Escalator()
    alert = make_alert()
    e.process(alert)
    e.process(alert)
    assert e.repeat_count(alert) == 2
