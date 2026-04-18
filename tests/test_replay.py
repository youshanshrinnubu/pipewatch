"""Tests for pipewatch.replay."""
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.replay import replay, replay_summary, ReplayEvent


def make_metric(pipeline="pipe", status="ok", total=100, failed=0):
    return PipelineMetric(
        pipeline_name=pipeline,
        status=status,
        total_records=total,
        failed_records=failed,
        duration_seconds=1.0,
    )


def test_replay_empty_returns_empty():
    assert replay([]) == []


def test_replay_single_healthy_no_alerts():
    events = replay([make_metric(failed=0)])
    assert len(events) == 1
    assert events[0].alerts == []


def test_replay_high_failure_rate_triggers_warn():
    events = replay([make_metric(failed=10, total=100)])
    severities = [a.severity for a in events[0].alerts]
    assert "warning" in severities


def test_replay_critical_failure_rate_triggers_critical():
    events = replay([make_metric(failed=25, total=100)])
    severities = [a.severity for a in events[0].alerts]
    assert "critical" in severities


def test_replay_error_status_triggers_critical():
    events = replay([make_metric(status="error", failed=0)])
    severities = [a.severity for a in events[0].alerts]
    assert "critical" in severities


def test_replay_index_assigned_correctly():
    metrics = [make_metric(pipeline=f"p{i}") for i in range(3)]
    events = replay(metrics)
    assert [e.index for e in events] == [0, 1, 2]


def test_replay_to_dict_keys():
    events = replay([make_metric(failed=5)])
    d = events[0].to_dict()
    assert "index" in d
    assert "pipeline" in d
    assert "alerts" in d
    assert "failure_rate" in d


def test_replay_summary_counts():
    metrics = [
        make_metric(failed=0),
        make_metric(failed=10),
        make_metric(failed=25),
        make_metric(status="error"),
    ]
    events = replay(metrics)
    s = replay_summary(events)
    assert s["total_events"] == 4
    assert s["events_with_alerts"] >= 3
    assert s["critical_events"] >= 2


def test_replay_summary_no_alerts():
    events = replay([make_metric(), make_metric()])
    s = replay_summary(events)
    assert s["events_with_alerts"] == 0
    assert s["critical_events"] == 0


def test_replay_custom_thresholds():
    # With very tight threshold, even 2% failure triggers warning
    events = replay([make_metric(failed=2, total=100)], warn_threshold=0.01)
    assert len(events[0].alerts) > 0
