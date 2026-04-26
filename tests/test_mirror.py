"""Tests for pipewatch.mirror."""
from __future__ import annotations

import time
from typing import List

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.mirror import Mirror, MirrorResult, MirrorRule


def make_metric(name: str = "test_pipeline", status: str = "ok") -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        status=status,
        total_records=100,
        failed_records=2,
        timestamp=time.time(),
    )


# ---------------------------------------------------------------------------
# MirrorRule.matches
# ---------------------------------------------------------------------------

def test_rule_none_prefix_matches_all():
    rule = MirrorRule(destination="backup")
    assert rule.matches(make_metric("any_pipeline")) is True


def test_rule_prefix_matches_correctly():
    rule = MirrorRule(destination="backup", pipeline_prefix="sales")
    assert rule.matches(make_metric("sales_etl")) is True


def test_rule_prefix_skips_non_matching():
    rule = MirrorRule(destination="backup", pipeline_prefix="sales")
    assert rule.matches(make_metric("inventory_sync")) is False


# ---------------------------------------------------------------------------
# Mirror.send
# ---------------------------------------------------------------------------

def test_send_calls_matching_handler():
    received: List[str] = []
    mirror = Mirror()
    mirror.register_destination("primary", lambda m: received.append(m.pipeline_name))
    mirror.add_rule(MirrorRule(destination="primary"))

    result = mirror.send(make_metric("etl_job"))

    assert "etl_job" in received
    assert "primary" in result.destinations
    assert result.skipped is False


def test_send_skipped_when_no_rules():
    mirror = Mirror()
    result = mirror.send(make_metric())
    assert result.skipped is True
    assert result.destinations == []


def test_send_skipped_when_prefix_no_match():
    received: List[str] = []
    mirror = Mirror()
    mirror.register_destination("d", lambda m: received.append(m.pipeline_name))
    mirror.add_rule(MirrorRule(destination="d", pipeline_prefix="sales"))

    result = mirror.send(make_metric("inventory_sync"))

    assert received == []
    assert result.skipped is True


def test_send_multiple_destinations():
    hits: List[str] = []
    mirror = Mirror()
    mirror.register_destination("a", lambda m: hits.append("a"))
    mirror.register_destination("b", lambda m: hits.append("b"))
    mirror.add_rule(MirrorRule(destination="a"))
    mirror.add_rule(MirrorRule(destination="b"))

    result = mirror.send(make_metric())

    assert set(result.destinations) == {"a", "b"}
    assert hits == ["a", "b"]


def test_send_destination_not_duplicated_in_result():
    mirror = Mirror()
    mirror.register_destination("a", lambda m: None)
    # Two rules pointing to the same destination
    mirror.add_rule(MirrorRule(destination="a"))
    mirror.add_rule(MirrorRule(destination="a"))

    result = mirror.send(make_metric())

    assert result.destinations.count("a") == 1


# ---------------------------------------------------------------------------
# Mirror.send_all
# ---------------------------------------------------------------------------

def test_send_all_returns_one_result_per_metric():
    mirror = Mirror()
    mirror.register_destination("x", lambda m: None)
    mirror.add_rule(MirrorRule(destination="x"))

    metrics = [make_metric(f"p{i}") for i in range(3)]
    results = mirror.send_all(metrics)

    assert len(results) == 3


# ---------------------------------------------------------------------------
# MirrorResult.to_dict
# ---------------------------------------------------------------------------

def test_to_dict_structure():
    m = make_metric("my_pipe")
    r = MirrorResult(metric=m, destinations=["primary"], skipped=False)
    d = r.to_dict()
    assert d["pipeline"] == "my_pipe"
    assert d["destinations"] == ["primary"]
    assert d["skipped"] is False
