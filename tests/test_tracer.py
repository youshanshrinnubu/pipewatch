import time
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.tracer import trace_pipeline, trace_all, format_trace, TraceEvent


def make_metric(pipeline="pipe", status="ok", total=1000, failed=10, ts=None):
    return PipelineMetric(
        pipeline_name=pipeline,
        timestamp=ts or time.time(),
        status=status,
        total_records=total,
        failed_records=failed,
        duration_seconds=1.0,
    )


def test_trace_empty_returns_empty():
    assert trace_pipeline([]) == []


def test_trace_single_metric_no_transition():
    m = make_metric(ts=100.0)
    events = trace_pipeline([m])
    assert len(events) == 1
    assert events[0].transition is None
    assert events[0].status == "ok"


def test_trace_detects_status_change():
    m1 = make_metric(status="ok", ts=100.0)
    m2 = make_metric(status="warning", ts=200.0)
    events = trace_pipeline([m1, m2])
    assert events[0].transition is None
    assert events[1].transition == "ok->warning"


def test_trace_no_transition_when_same_status():
    m1 = make_metric(status="ok", ts=100.0)
    m2 = make_metric(status="ok", ts=200.0)
    events = trace_pipeline([m1, m2])
    assert events[1].transition is None


def test_trace_sorted_by_timestamp():
    m1 = make_metric(status="warning", ts=200.0)
    m2 = make_metric(status="ok", ts=100.0)
    events = trace_pipeline([m1, m2])
    assert events[0].timestamp == 100.0
    assert events[0].status == "ok"
    assert events[1].transition == "ok->warning"


def test_trace_failure_rate_calculated():
    m = make_metric(total=1000, failed=100, ts=1.0)
    events = trace_pipeline([m])
    assert abs(events[0].failure_rate - 0.1) < 1e-6


def test_trace_failure_rate_zero_records():
    m = make_metric(total=0, failed=0, ts=1.0)
    events = trace_pipeline([m])
    assert events[0].failure_rate == 0.0


def test_trace_all_groups_by_pipeline():
    m1 = make_metric(pipeline="a", ts=1.0)
    m2 = make_metric(pipeline="b", ts=2.0)
    m3 = make_metric(pipeline="a", ts=3.0)
    result = trace_all([m1, m2, m3])
    assert set(result.keys()) == {"a", "b"}
    assert len(result["a"]) == 2
    assert len(result["b"]) == 1


def test_to_dict_contains_fields():
    e = TraceEvent(pipeline="p", timestamp=1.0, status="ok", failure_rate=0.05, transition="warning->ok")
    d = e.to_dict()
    assert d["pipeline"] == "p"
    assert d["transition"] == "warning->ok"
    assert d["failure_rate"] == 0.05


def test_format_trace_empty():
    assert "no events" in format_trace([])


def test_format_trace_shows_transition():
    e = TraceEvent(pipeline="p", timestamp=1.0, status="warning", failure_rate=0.1, transition="ok->warning")
    out = format_trace([e])
    assert "ok->warning" in out


def test_trace_multiple_transitions():
    """Verify that a sequence of status changes produces the correct chain of transitions."""
    m1 = make_metric(status="ok", ts=1.0)
    m2 = make_metric(status="warning", ts=2.0)
    m3 = make_metric(status="error", ts=3.0)
    m4 = make_metric(status="ok", ts=4.0)
    events = trace_pipeline([m1, m2, m3, m4])
    assert events[0].transition is None
    assert events[1].transition == "ok->warning"
    assert events[2].transition == "warning->error"
    assert events[3].transition == "error->ok"
