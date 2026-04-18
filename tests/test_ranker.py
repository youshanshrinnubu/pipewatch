"""Tests for pipewatch.ranker."""
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.ranker import rank_by_score, rank_by_failure_rate, format_ranked


def make_metric(pipeline="pipe", status="ok", records=100, failures=0):
    return PipelineMetric(
        pipeline=pipeline,
        status=status,
        records_processed=records,
        records_failed=failures,
        duration_seconds=1.0,
    )


def test_rank_by_score_empty():
    assert rank_by_score([]) == []


def test_rank_by_failure_rate_empty():
    assert rank_by_failure_rate([]) == []


def test_rank_by_score_worst_first():
    m1 = make_metric("good", records=100, failures=0)
    m2 = make_metric("bad", records=100, failures=80)
    ranked = rank_by_score([m1, m2], ascending=True)
    assert ranked[0].pipeline == "bad"
    assert ranked[1].pipeline == "good"


def test_rank_by_score_best_first():
    m1 = make_metric("good", records=100, failures=0)
    m2 = make_metric("bad", records=100, failures=80)
    ranked = rank_by_score([m1, m2], ascending=False)
    assert ranked[0].pipeline == "good"


def test_rank_by_score_assigns_sequential_ranks():
    metrics = [make_metric(f"p{i}", records=100, failures=i * 10) for i in range(3)]
    ranked = rank_by_score(metrics)
    assert [r.rank for r in ranked] == [1, 2, 3]


def test_rank_by_failure_rate_highest_first():
    m1 = make_metric("low", records=100, failures=5)
    m2 = make_metric("high", records=100, failures=50)
    ranked = rank_by_failure_rate([m1, m2])
    assert ranked[0].pipeline == "high"
    assert ranked[0].failure_rate == pytest.approx(0.5)


def test_rank_by_failure_rate_lowest_first():
    m1 = make_metric("low", records=100, failures=5)
    m2 = make_metric("high", records=100, failures=50)
    ranked = rank_by_failure_rate([m1, m2], ascending=True)
    assert ranked[0].pipeline == "low"


def test_ranked_to_dict():
    m = make_metric("pipe", records=100, failures=10)
    ranked = rank_by_score([m])
    d = ranked[0].to_dict()
    assert d["rank"] == 1
    assert d["pipeline"] == "pipe"
    assert "score" in d
    assert "failure_rate" in d


def test_format_ranked_empty():
    result = format_ranked([])
    assert "No pipelines" in result


def test_format_ranked_contains_pipeline_name():
    m = make_metric("mypipe", records=100, failures=5)
    ranked = rank_by_score([m])
    out = format_ranked(ranked)
    assert "mypipe" in out
    assert "1" in out
