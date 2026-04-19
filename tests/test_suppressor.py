"""Tests for pipewatch.suppressor."""
import pytest
from pipewatch.alerts import Alert
from pipewatch.suppressor import SuppressionRule, Suppressor, SuppressResult


def make_alert(pipeline="sales", severity="warning", message="high failure rate"):
    return Alert(pipeline=pipeline, severity=severity, message=message)


def test_rule_matches_exact():
    rule = SuppressionRule(pipeline="sales", severity="warning", reason="planned")
    alert = make_alert(pipeline="sales", severity="warning")
    assert rule.matches(alert)


def test_rule_no_match_wrong_pipeline():
    rule = SuppressionRule(pipeline="inventory", severity="warning")
    alert = make_alert(pipeline="sales", severity="warning")
    assert not rule.matches(alert)


def test_rule_no_match_wrong_severity():
    rule = SuppressionRule(pipeline="sales", severity="critical")
    alert = make_alert(pipeline="sales", severity="warning")
    assert not rule.matches(alert)


def test_rule_none_pipeline_matches_all():
    rule = SuppressionRule(pipeline=None, severity="warning")
    for name in ["sales", "inventory", "orders"]:
        assert rule.matches(make_alert(pipeline=name, severity="warning"))


def test_rule_none_severity_matches_all():
    rule = SuppressionRule(pipeline="sales", severity=None)
    for sev in ["warning", "critical"]:
        assert rule.matches(make_alert(pipeline="sales", severity=sev))


def test_check_suppressed():
    sup = Suppressor([SuppressionRule(pipeline="sales", severity="warning", reason="ok")])
    result = sup.check(make_alert())
    assert result.suppressed
    assert result.reason == "ok"


def test_check_not_suppressed():
    sup = Suppressor()
    result = sup.check(make_alert())
    assert not result.suppressed


def test_filter_returns_all_results():
    sup = Suppressor([SuppressionRule(pipeline="sales", severity=None, reason="maint")])
    alerts = [make_alert("sales"), make_alert("inventory")]
    results = sup.filter(alerts)
    assert len(results) == 2
    assert results[0].suppressed
    assert not results[1].suppressed


def test_allowed_excludes_suppressed():
    sup = Suppressor([SuppressionRule(pipeline="sales", severity=None)])
    alerts = [make_alert("sales"), make_alert("inventory")]
    allowed = sup.allowed(alerts)
    assert len(allowed) == 1
    assert allowed[0].pipeline == "inventory"


def test_to_dict():
    from pipewatch.suppressor import to_dict
    rule = SuppressionRule(pipeline="sales", severity="warning", reason="planned maintenance")
    d = to_dict(rule)
    assert d["pipeline"] == "sales"
    assert d["severity"] == "warning"
    assert d["reason"] == "planned maintenance"


def test_suppress_result_to_dict():
    alert = make_alert()
    r = SuppressResult(alert=alert, suppressed=True, reason="test")
    d = r.to_dict()
    assert d["suppressed"] is True
    assert d["pipeline"] == "sales"
