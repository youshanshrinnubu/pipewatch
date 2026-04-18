"""Tests for pipewatch.silencer."""

from datetime import datetime, timedelta

import pytest

from pipewatch.alerts import Alert
from pipewatch.silencer import SilenceRule, Silencer


def make_alert(pipeline="pipe_a", severity="warning", message="oops"):
    return Alert(pipeline=pipeline, severity=severity, message=message)


def test_rule_matches_pipeline_and_severity():
    rule = SilenceRule(pipeline="pipe_a", severity="warning")
    assert rule.matches(make_alert(pipeline="pipe_a", severity="warning"))


def test_rule_no_match_wrong_pipeline():
    rule = SilenceRule(pipeline="pipe_b", severity="warning")
    assert not rule.matches(make_alert(pipeline="pipe_a", severity="warning"))


def test_rule_no_match_wrong_severity():
    rule = SilenceRule(pipeline="pipe_a", severity="critical")
    assert not rule.matches(make_alert(pipeline="pipe_a", severity="warning"))


def test_rule_none_pipeline_matches_all():
    rule = SilenceRule(pipeline=None, severity="warning")
    assert rule.matches(make_alert(pipeline="any_pipe", severity="warning"))


def test_rule_expired_does_not_match():
    past = datetime.utcnow() - timedelta(seconds=1)
    rule = SilenceRule(pipeline="pipe_a", expires_at=past)
    assert not rule.matches(make_alert(pipeline="pipe_a"))


def test_rule_not_expired_matches():
    future = datetime.utcnow() + timedelta(hours=1)
    rule = SilenceRule(pipeline="pipe_a", expires_at=future)
    assert rule.matches(make_alert(pipeline="pipe_a"))


def test_silencer_filters_silenced_alerts():
    silencer = Silencer()
    silencer.add_rule(SilenceRule(pipeline="pipe_a"))
    alerts = [make_alert("pipe_a"), make_alert("pipe_b")]
    result = silencer.filter(alerts)
    assert len(result) == 1
    assert result[0].pipeline == "pipe_b"


def test_silencer_is_silenced_true():
    silencer = Silencer()
    silencer.add_rule(SilenceRule(severity="critical"))
    assert silencer.is_silenced(make_alert(severity="critical"))


def test_silencer_is_silenced_false():
    silencer = Silencer()
    silencer.add_rule(SilenceRule(severity="critical"))
    assert not silencer.is_silenced(make_alert(severity="warning"))


def test_purge_expired_removes_old_rules():
    past = datetime.utcnow() - timedelta(seconds=1)
    future = datetime.utcnow() + timedelta(hours=1)
    silencer = Silencer()
    silencer.add_rule(SilenceRule(pipeline="old", expires_at=past))
    silencer.add_rule(SilenceRule(pipeline="new", expires_at=future))
    removed = silencer.purge_expired()
    assert removed == 1
    assert len(silencer.active_rules()) == 1


def test_to_dict_contains_fields():
    rule = SilenceRule(pipeline="pipe_a", severity="warning", reason="maintenance")
    d = rule.to_dict()
    assert d["pipeline"] == "pipe_a"
    assert d["severity"] == "warning"
    assert d["reason"] == "maintenance"
    assert d["expires_at"] is None
