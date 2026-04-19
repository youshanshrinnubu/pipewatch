import pytest
from pipewatch.alerts import Alert
from pipewatch.correlator import (
    correlate_by_severity,
    correlate_by_message,
    format_correlation,
    CorrelationGroup,
)


def make_alert(pipeline="pipe_a", severity="warning", message="high failure rate") -> Alert:
    return Alert(pipeline=pipeline, severity=severity, message=message)


def test_correlate_by_severity_groups_multiple_pipelines():
    alerts = [
        make_alert("pipe_a", "warning"),
        make_alert("pipe_b", "warning"),
        make_alert("pipe_c", "critical"),
    ]
    groups = correlate_by_severity(alerts)
    assert len(groups) == 1
    assert groups[0].severity == "warning"
    assert set(groups[0].pipelines) == {"pipe_a", "pipe_b"}


def test_correlate_by_severity_skips_single_pipeline():
    alerts = [make_alert("pipe_a", "critical"), make_alert("pipe_a", "critical")]
    groups = correlate_by_severity(alerts)
    assert groups == []


def test_correlate_by_severity_empty():
    assert correlate_by_severity([]) == []


def test_correlate_by_message_groups_shared_message():
    alerts = [
        make_alert("pipe_a", "warning", "disk full"),
        make_alert("pipe_b", "critical", "disk full"),
        make_alert("pipe_c", "warning", "timeout"),
    ]
    groups = correlate_by_message(alerts)
    assert len(groups) == 1
    assert groups[0].message_sample == "disk full"
    assert set(groups[0].pipelines) == {"pipe_a", "pipe_b"}


def test_correlate_by_message_respects_min_pipelines():
    alerts = [
        make_alert("pipe_a", "warning", "slow"),
        make_alert("pipe_b", "warning", "slow"),
        make_alert("pipe_c", "warning", "slow"),
    ]
    groups = correlate_by_message(alerts, min_pipelines=3)
    assert len(groups) == 1
    assert len(groups[0].pipelines) == 3


def test_correlate_by_message_empty():
    assert correlate_by_message([]) == []


def test_to_dict_contains_expected_keys():
    alerts = [make_alert("a"), make_alert("b")]
    groups = correlate_by_severity(alerts)
    assert len(groups) == 1
    d = groups[0].to_dict()
    assert "severity" in d
    assert "pipelines" in d
    assert "alert_count" in d
    assert "message_sample" in d


def test_format_correlation_no_groups():
    result = format_correlation([])
    assert "No correlated" in result


def test_format_correlation_with_groups():
    alerts = [make_alert("pipe_a", "critical", "crash"), make_alert("pipe_b", "critical", "crash")]
    groups = correlate_by_severity(alerts)
    output = format_correlation(groups)
    assert "CRITICAL" in output
    assert "pipe_a" in output or "pipe_b" in output
