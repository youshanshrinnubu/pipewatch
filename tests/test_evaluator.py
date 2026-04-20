"""Tests for pipewatch.evaluator."""
from __future__ import annotations

from pipewatch.evaluator import (
    EvaluationResult,
    EvaluatorConfig,
    evaluate,
    evaluate_all,
    format_evaluation,
)
from pipewatch.metrics import PipelineMetric


def make_metric(
    pipeline: str = "pipe",
    total: int = 100,
    failed: int = 0,
    status: str = "ok",
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        total_records=total,
        failed_records=failed,
        status=status,
    )


# --- evaluate() ---

def test_evaluate_healthy_returns_score_one():
    result = evaluate(make_metric(failed=0))
    assert result.score == 1.0
    assert result.tier == "healthy"
    assert result.reasons == []


def test_evaluate_error_status_is_zero():
    result = evaluate(make_metric(status="error"))
    assert result.score == 0.0
    assert result.tier == "critical"
    assert any("error" in r for r in result.reasons)


def test_evaluate_warning_status_penalises_score():
    result = evaluate(make_metric(failed=2, status="warning"))
    assert result.score < 1.0
    assert any("warning" in r for r in result.reasons)


def test_evaluate_high_failure_rate_critical():
    result = evaluate(make_metric(failed=30, total=100))
    assert result.tier == "critical"
    assert result.score < 0.30


def test_evaluate_moderate_failure_rate_warning():
    result = evaluate(make_metric(failed=10, total=100))
    assert result.tier == "warning"
    assert 0.30 <= result.score < 0.60


def test_evaluate_score_clamped_to_zero():
    result = evaluate(make_metric(failed=100, total=100, status="error"))
    assert result.score == 0.0


def test_evaluate_custom_config_thresholds():
    cfg = EvaluatorConfig(warning_failure_rate=0.01, critical_failure_rate=0.10)
    result = evaluate(make_metric(failed=5, total=100), cfg)
    # 5% >= warning threshold 1% but < critical 10% → warning tier
    assert result.tier == "warning"


def test_evaluate_pipeline_name_preserved():
    result = evaluate(make_metric(pipeline="my_pipe"))
    assert result.pipeline == "my_pipe"


# --- evaluate_all() ---

def test_evaluate_all_returns_one_per_metric():
    metrics = [make_metric(pipeline=f"p{i}") for i in range(4)]
    results = evaluate_all(metrics)
    assert len(results) == 4


def test_evaluate_all_empty_returns_empty():
    assert evaluate_all([]) == []


# --- to_dict() ---

def test_to_dict_contains_expected_keys():
    result = evaluate(make_metric())
    d = result.to_dict()
    assert set(d.keys()) == {"pipeline", "score", "tier", "reasons"}


# --- format_evaluation() ---

def test_format_evaluation_empty():
    assert format_evaluation([]) == "No evaluation results."


def test_format_evaluation_contains_pipeline_name():
    result = evaluate(make_metric(pipeline="alpha"))
    text = format_evaluation([result])
    assert "alpha" in text


def test_format_evaluation_contains_tier():
    result = evaluate(make_metric(failed=50, total=100))
    text = format_evaluation([result])
    assert "CRITICAL" in text
