import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.enricher import Enricher, EnrichedMetric, EnrichmentRule


def make_metric(name="pipe_a", status="ok", processed=100, failed=0):
    return PipelineMetric(name, status, processed, failed)


def test_enrich_applies_global_rule():
    enricher = Enricher()
    enricher.add_rule("env", "prod")
    result = enricher.enrich(make_metric())
    assert isinstance(result, EnrichedMetric)
    assert result.meta["env"] == "prod"


def test_enrich_applies_prefix_rule_match():
    enricher = Enricher()
    enricher.add_rule("team", "data", pipeline_prefix="sales")
    result = enricher.enrich(make_metric(name="sales_etl"))
    assert result.meta["team"] == "data"


def test_enrich_skips_prefix_rule_no_match():
    enricher = Enricher()
    enricher.add_rule("team", "data", pipeline_prefix="sales")
    result = enricher.enrich(make_metric(name="inventory_sync"))
    assert "team" not in result.meta


def test_enrich_multiple_rules():
    enricher = Enricher()
    enricher.add_rule("env", "staging")
    enricher.add_rule("owner", "alice", pipeline_prefix="sales")
    result = enricher.enrich(make_metric(name="sales_daily"))
    assert result.meta["env"] == "staging"
    assert result.meta["owner"] == "alice"


def test_has_meta_true():
    enricher = Enricher()
    enricher.add_rule("env", "prod")
    result = enricher.enrich(make_metric())
    assert result.has_meta("env") is True


def test_has_meta_false():
    enricher = Enricher()
    result = enricher.enrich(make_metric())
    assert result.has_meta("env") is False


def test_enrich_all_returns_list():
    enricher = Enricher()
    enricher.add_rule("env", "prod")
    metrics = [make_metric(name=f"pipe_{i}") for i in range(3)]
    results = enricher.enrich_all(metrics)
    assert len(results) == 3
    assert all(r.meta["env"] == "prod" for r in results)


def test_to_dict_contains_meta():
    enricher = Enricher()
    enricher.add_rule("env", "test")
    result = enricher.enrich(make_metric())
    d = result.to_dict()
    assert d["meta"]["env"] == "test"
    assert d["pipeline"] == "pipe_a"
    assert d["status"] == "ok"


def test_enrich_empty_rules():
    enricher = Enricher()
    result = enricher.enrich(make_metric())
    assert result.meta == {}
