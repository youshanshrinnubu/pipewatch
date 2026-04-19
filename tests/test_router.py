"""Tests for pipewatch.router."""

import pytest
from pipewatch.router import Router, RouteRule
from pipewatch.alerts import Alert


def make_alert(pipeline="pipe", severity="warning", message="msg"):
    return Alert(pipeline=pipeline, severity=severity, message=message)


def test_dispatch_calls_matching_handler():
    called = []
    router = Router()
    router.add_rule(lambda a: called.append(a), severity="warning")
    alert = make_alert(severity="warning")
    count = router.dispatch(alert)
    assert count == 1
    assert called == [alert]


def test_dispatch_skips_non_matching_severity():
    called = []
    router = Router()
    router.add_rule(lambda a: called.append(a), severity="critical")
    router.dispatch(make_alert(severity="warning"))
    assert called == []


def test_dispatch_uses_fallback_when_no_match():
    called = []
    router = Router()
    router.add_rule(lambda a: called.append(("rule", a)), severity="critical")
    router.set_fallback(lambda a: called.append(("fallback", a)))
    alert = make_alert(severity="warning")
    count = router.dispatch(alert)
    assert count == 1
    assert called[0][0] == "fallback"


def test_dispatch_multiple_matching_rules():
    called = []
    router = Router()
    router.add_rule(lambda a: called.append(1))
    router.add_rule(lambda a: called.append(2))
    router.dispatch(make_alert())
    assert called == [1, 2]


def test_pipeline_prefix_rule_matches():
    called = []
    router = Router()
    router.add_rule(lambda a: called.append(a), pipeline_prefix="ing")
    router.dispatch(make_alert(pipeline="ingest"))
    assert len(called) == 1


def test_pipeline_prefix_rule_no_match():
    called = []
    router = Router()
    router.add_rule(lambda a: called.append(a), pipeline_prefix="transform")
    router.dispatch(make_alert(pipeline="ingest"))
    assert called == []


def test_dispatch_all_returns_total_count():
    router = Router()
    router.add_rule(lambda a: None)
    alerts = [make_alert(), make_alert(), make_alert()]
    total = router.dispatch_all(alerts)
    assert total == 3


def test_no_match_no_fallback_returns_zero():
    router = Router()
    router.add_rule(lambda a: None, severity="critical")
    count = router.dispatch(make_alert(severity="warning"))
    assert count == 0
