import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.tagger import Tagger, TaggedMetric, filter_by_tag


def make_metric(name="pipe_a", status="ok", total=100, failed=2):
    return PipelineMetric(
        pipeline_name=name,
        status=status,
        total_records=total,
        failed_records=failed,
        duration_seconds=1.0,
    )


def test_tag_applies_global_rule():
    tagger = Tagger()
    tagger.add_rule("env", "prod")
    result = tagger.tag(make_metric("pipe_a"))
    assert result.tags["env"] == "prod"


def test_tag_applies_prefix_rule_match():
    tagger = Tagger()
    tagger.add_rule("team", "data", pipeline_prefix="pipe_")
    result = tagger.tag(make_metric("pipe_orders"))
    assert result.tags["team"] == "data"


def test_tag_skips_prefix_rule_no_match():
    tagger = Tagger()
    tagger.add_rule("team", "data", pipeline_prefix="etl_")
    result = tagger.tag(make_metric("pipe_orders"))
    assert "team" not in result.tags


def test_tag_all_returns_all_tagged():
    tagger = Tagger()
    tagger.add_rule("env", "staging")
    metrics = [make_metric("a"), make_metric("b")]
    results = tagger.tag_all(metrics)
    assert len(results) == 2
    assert all(r.tags["env"] == "staging" for r in results)


def test_has_tag_key_only():
    tm = TaggedMetric(metric=make_metric(), tags={"env": "prod"})
    assert tm.has_tag("env")
    assert not tm.has_tag("team")


def test_has_tag_key_and_value():
    tm = TaggedMetric(metric=make_metric(), tags={"env": "prod"})
    assert tm.has_tag("env", "prod")
    assert not tm.has_tag("env", "staging")


def test_filter_by_tag_returns_matching():
    tagger = Tagger()
    tagger.add_rule("region", "us", pipeline_prefix="us_")
    metrics = [make_metric("us_pipe"), make_metric("eu_pipe")]
    tagged = tagger.tag_all(metrics)
    result = filter_by_tag(tagged, "region", "us")
    assert len(result) == 1
    assert result[0].metric.pipeline_name == "us_pipe"


def test_to_dict_contains_tags():
    tm = TaggedMetric(metric=make_metric("p", total=200, failed=10), tags={"env": "prod"})
    d = tm.to_dict()
    assert d["tags"] == {"env": "prod"}
    assert d["pipeline"] == "p"
    assert d["failure_rate"] == 0.05
