"""Tests for pipewatch.mapper."""
from __future__ import annotations

import dataclasses
from datetime import datetime, timezone

import pytest

from pipewatch.mapper import MapResult, MapRule, Mapper, format_mapped
from pipewatch.metrics import PipelineMetric


def make_metric(
    name="sales",
    status="ok",
    total=100,
    failed=0,
) -> PipelineMetric:
    return PipelineMetric(name, status, total, failed, datetime.now(timezone.utc))


# --- MapRule.matches ---

def test_rule_none_prefix_matches_all():
    rule = MapRule(pipeline_prefix=None, transform=lambda m: m, label="all")
    assert rule.matches(make_metric("anything"))


def test_rule_prefix_matches_correctly():
    rule = MapRule(pipeline_prefix="sales", transform=lambda m: m, label="sales")
    assert rule.matches(make_metric("sales_daily"))
    assert not rule.matches(make_metric("inventory_sync"))


# --- Mapper.apply ---

def test_apply_no_rules_returns_unchanged():
    mapper = Mapper()
    m = make_metric()
    result = mapper.apply(m)
    assert result.mapped is m
    assert not result.changed
    assert result.rules_applied == []


def test_apply_single_rule_transforms_metric():
    def bump(m: PipelineMetric) -> PipelineMetric:
        d = dataclasses.asdict(m)
        d["failed_records"] = m.failed_records + 10
        return PipelineMetric(**d)

    mapper = Mapper()
    mapper.add_rule(MapRule(pipeline_prefix=None, transform=bump, label="bump"))
    m = make_metric(failed=5)
    result = mapper.apply(m)
    assert result.mapped.failed_records == 15
    assert "bump" in result.rules_applied
    assert result.changed


def test_apply_rule_skips_non_matching_prefix():
    def zero_failed(m: PipelineMetric) -> PipelineMetric:
        d = dataclasses.asdict(m)
        d["failed_records"] = 0
        return PipelineMetric(**d)

    mapper = Mapper()
    mapper.add_rule(MapRule(pipeline_prefix="orders", transform=zero_failed, label="zero"))
    m = make_metric(name="sales_daily", failed=50)
    result = mapper.apply(m)
    assert result.mapped.failed_records == 50
    assert result.rules_applied == []


def test_apply_all_returns_one_result_per_metric():
    mapper = Mapper()
    metrics = [make_metric(f"pipe_{i}") for i in range(4)]
    results = mapper.apply_all(metrics)
    assert len(results) == 4
    assert all(isinstance(r, MapResult) for r in results)


# --- MapResult.to_dict ---

def test_to_dict_contains_expected_keys():
    m = make_metric()
    result = MapResult(original=m, mapped=m, rules_applied=[])
    d = result.to_dict()
    for key in ("pipeline", "rules_applied", "changed", "original_status", "mapped_status"):
        assert key in d


# --- format_mapped ---

def test_format_mapped_text_no_results():
    output = format_mapped([])
    assert "No metrics" in output


def test_format_mapped_text_contains_pipeline_name():
    mapper = Mapper()
    m = make_metric(name="my_pipeline")
    results = mapper.apply_all([m])
    output = format_mapped(results, fmt="text")
    assert "my_pipeline" in output


def test_format_mapped_json_valid():
    import json
    mapper = Mapper()
    results = mapper.apply_all([make_metric()])
    output = format_mapped(results, fmt="json")
    data = json.loads(output)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "sales"
