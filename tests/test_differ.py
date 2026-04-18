import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.differ import diff_metrics, diff_all, MetricDiff


def make_metric(pipeline="pipe", total=100, failed=5, status="ok"):
    return PipelineMetric(
        pipeline=pipeline,
        total_records=total,
        failed_records=failed,
        status=status,
        duration_seconds=1.0,
        timestamp="2024-01-01T00:00:00",
    )


def test_diff_metrics_no_change():
    m = make_metric(failed=10)
    d = diff_metrics(m, m)
    assert d.failure_rate_delta == pytest.approx(0.0)
    assert not d.status_changed
    assert not d.is_degraded()


def test_diff_metrics_degraded_on_higher_failure_rate():
    prev = make_metric(failed=5)
    curr = make_metric(failed=20)
    d = diff_metrics(prev, curr)
    assert d.failure_rate_delta > 0
    assert d.is_degraded()


def test_diff_metrics_status_changed():
    prev = make_metric(status="ok")
    curr = make_metric(status="error")
    d = diff_metrics(prev, curr)
    assert d.status_changed
    assert d.is_degraded()


def test_diff_metrics_improvement_not_degraded():
    prev = make_metric(failed=30)
    curr = make_metric(failed=5)
    d = diff_metrics(prev, curr)
    assert d.failure_rate_delta < 0
    assert not d.is_degraded()


def test_diff_all_matches_by_pipeline():
    prev = [make_metric("a", failed=5), make_metric("b", failed=2)]
    curr = [make_metric("a", failed=20), make_metric("b", failed=2)]
    diffs = diff_all(prev, curr)
    assert len(diffs) == 2
    names = {d.pipeline for d in diffs}
    assert names == {"a", "b"}


def test_diff_all_skips_new_pipelines():
    prev = [make_metric("a")]
    curr = [make_metric("a"), make_metric("new_pipe")]
    diffs = diff_all(prev, curr)
    assert len(diffs) == 1
    assert diffs[0].pipeline == "a"


def test_diff_all_empty_inputs():
    assert diff_all([], []) == []
    assert diff_all([], [make_metric("a")]) == []
    assert diff_all([make_metric("a")], []) == []


def test_to_dict_contains_expected_keys():
    d = diff_metrics(make_metric(failed=5), make_metric(failed=10))
    result = d.to_dict()
    for key in ("pipeline", "prev_failure_rate", "curr_failure_rate",
                "failure_rate_delta", "status_changed", "degraded"):
        assert key in result


def test_to_dict_values_match_attributes():
    prev = make_metric(failed=5)
    curr = make_metric(failed=10)
    d = diff_metrics(prev, curr)
    result = d.to_dict()
    assert result["pipeline"] == d.pipeline
    assert result["failure_rate_delta"] == pytest.approx(d.failure_rate_delta)
    assert result["status_changed"] == d.status_changed
    assert result["degraded"] == d.is_degraded()
