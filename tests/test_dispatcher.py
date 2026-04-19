import pytest
from pipewatch.alerts import Alert
from pipewatch.dispatcher import DispatchRule, Dispatcher, DispatchResult, format_dispatch_results


def make_alert(pipeline="pipe", severity="warning", message="msg"):
    return Alert(pipeline=pipeline, severity=severity, message=message)


def test_dispatch_calls_matching_handler():
    received = []
    rule = DispatchRule(severity="warning", pipeline=None, handler=lambda a: received.append(a), label="w")
    d = Dispatcher()
    d.add_rule(rule)
    result = d.dispatch(make_alert(severity="warning"))
    assert len(received) == 1
    assert result.dispatched is True
    assert "w" in result.matched_labels


def test_dispatch_skips_non_matching_severity():
    received = []
    rule = DispatchRule(severity="critical", pipeline=None, handler=lambda a: received.append(a), label="c")
    d = Dispatcher()
    d.add_rule(rule)
    result = d.dispatch(make_alert(severity="warning"))
    assert len(received) == 0
    assert result.dispatched is False


def test_dispatch_uses_fallback_when_no_match():
    received = []
    d = Dispatcher()
    d.set_fallback(lambda a: received.append(a))
    result = d.dispatch(make_alert(severity="info"))
    assert len(received) == 1
    assert result.dispatched is True
    assert "(fallback)" in result.matched_labels


def test_dispatch_matches_pipeline_filter():
    received = []
    rule = DispatchRule(severity=None, pipeline="sales", handler=lambda a: received.append(a), label="sales-only")
    d = Dispatcher()
    d.add_rule(rule)
    d.dispatch(make_alert(pipeline="inventory"))
    assert len(received) == 0
    d.dispatch(make_alert(pipeline="sales"))
    assert len(received) == 1


def test_dispatch_all_returns_results_for_each():
    d = Dispatcher()
    alerts = [make_alert(pipeline=f"p{i}") for i in range(3)]
    results = d.dispatch_all(alerts)
    assert len(results) == 3


def test_multiple_rules_can_match():
    received = []
    d = Dispatcher()
    d.add_rule(DispatchRule(severity="warning", pipeline=None, handler=lambda a: received.append("r1"), label="r1"))
    d.add_rule(DispatchRule(severity=None, pipeline="pipe", handler=lambda a: received.append("r2"), label="r2"))
    result = d.dispatch(make_alert(pipeline="pipe", severity="warning"))
    assert "r1" in result.matched_labels
    assert "r2" in result.matched_labels
    assert len(received) == 2


def test_to_dict_contains_expected_keys():
    d = Dispatcher()
    result = d.dispatch(make_alert())
    data = result.to_dict()
    assert "pipeline" in data
    assert "severity" in data
    assert "dispatched" in data
    assert "matched_labels" in data


def test_format_empty_results():
    assert format_dispatch_results([]) == "No alerts dispatched."


def test_format_results_contains_pipeline():
    d = Dispatcher()
    d.set_fallback(lambda a: None)
    results = d.dispatch_all([make_alert(pipeline="mypipe")])
    text = format_dispatch_results(results)
    assert "mypipe" in text
