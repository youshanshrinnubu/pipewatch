import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.scorer import score_metric, score_all, best, worst, _grade


def make_metric(pipeline="pipe", status="ok", total=100, failed=0, failure_rate=None):
    fr = failure_rate if failure_rate is not None else (failed / total if total else 0.0)
    return PipelineMetric(
        pipeline=pipeline,
        status=status,
        total_records=total,
        failed_records=failed,
        failure_rate=fr,
        timestamp="2024-01-01T00:00:00",
    )


def test_grade_boundaries():
    assert _grade(95) == "A"
    assert _grade(80) == "B"
    assert _grade(65) == "C"
    assert _grade(50) == "D"
    assert _grade(30) == "F"


def test_score_perfect_metric():
    s = score_metric(make_metric(status="ok", total=100, failed=0, failure_rate=0.0))
    assert s.score == 100.0
    assert s.grade == "A"


def test_score_error_status_is_zero():
    s = score_metric(make_metric(status="error"))
    assert s.score == 0.0
    assert s.grade == "F"
    assert "error" in s.reason


def test_score_high_failure_rate():
    s = score_metric(make_metric(status="ok", total=100, failed=80, failure_rate=0.8))
    assert s.score == 20.0
    assert s.grade == "F"


def test_score_warning_status_penalised():
    s_ok = score_metric(make_metric(status="ok", failure_rate=0.05))
    s_warn = score_metric(make_metric(status="warning", failure_rate=0.05))
    assert s_warn.score < s_ok.score


def test_score_all_returns_list():
    metrics = [make_metric(f"p{i}", failure_rate=i * 0.1) for i in range(4)]
    scored = score_all(metrics)
    assert len(scored) == 4
    assert all(0.0 <= s.score <= 100.0 for s in scored)


def test_best_and_worst():
    metrics = [
        make_metric("good", failure_rate=0.0),
        make_metric("bad", status="error"),
    ]
    scored = score_all(metrics)
    assert best(scored).pipeline == "good"
    assert worst(scored).pipeline == "bad"


def test_best_worst_empty():
    assert best([]) is None
    assert worst([]) is None


def test_to_dict_keys():
    s = score_metric(make_metric())
    d = s.to_dict()
    assert set(d.keys()) == {"pipeline", "score", "grade", "reason"}
