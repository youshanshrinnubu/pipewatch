"""Tests for pipewatch/rechecker.py"""
import time
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.rechecker import recheck_pipeline, recheck_all, RecoveryResult


def make_metric(
    pipeline: str = "etl",
    total: int = 100,
    failed: int = 0,
    status: str = "ok",
    ts: float = None,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        total_records=total,
        failed_records=failed,
        status=status,
        timestamp=ts if ts is not None else time.time(),
    )


def test_recheck_returns_none_for_missing_pipeline():
    before = [make_metric("etl")]
    after = [make_metric("other")]
    assert recheck_pipeline("etl", before, after) is None


def test_recheck_returns_none_when_before_missing():
    before = []
    after = [make_metric("etl")]
    assert recheck_pipeline("etl", before, after) is None


def test_stable_healthy_pipeline():
    before = [make_metric("etl", total=100, failed=0, status="ok")]
    after = [make_metric("etl", total=100, failed=0, status="ok")]
    result = recheck_pipeline("etl", before, after)
    assert result is not None
    assert result.recovered is False
    assert result.still_failing is False
    assert result.note == "stable"


def test_recovered_pipeline():
    before = [make_metric("etl", total=100, failed=50, status="error")]
    after = [make_metric("etl", total=100, failed=0, status="ok")]
    result = recheck_pipeline("etl", before, after)
    assert result is not None
    assert result.recovered is True
    assert result.still_failing is False
    assert result.note == "pipeline recovered"


def test_still_failing_pipeline():
    before = [make_metric("etl", total=100, failed=50, status="error")]
    after = [make_metric("etl", total=100, failed=40, status="error")]
    result = recheck_pipeline("etl", before, after)
    assert result is not None
    assert result.still_failing is True
    assert result.recovered is False
    assert result.note == "pipeline still failing"


def test_newly_degraded_pipeline():
    before = [make_metric("etl", total=100, failed=0, status="ok")]
    after = [make_metric("etl", total=100, failed=80, status="error")]
    result = recheck_pipeline("etl", before, after)
    assert result is not None
    assert result.recovered is False
    assert result.still_failing is False
    assert result.note == "pipeline newly degraded"


def test_uses_most_recent_metric():
    t1 = 1_000_000.0
    t2 = 2_000_000.0
    before = [
        make_metric("etl", total=100, failed=0, status="ok", ts=t1),
        make_metric("etl", total=100, failed=60, status="error", ts=t2),
    ]
    after = [make_metric("etl", total=100, failed=0, status="ok")]
    result = recheck_pipeline("etl", before, after)
    assert result is not None
    assert result.previous_status == "error"
    assert result.recovered is True


def test_to_dict_keys():
    before = [make_metric("etl", total=100, failed=50, status="error")]
    after = [make_metric("etl", total=100, failed=0, status="ok")]
    result = recheck_pipeline("etl", before, after)
    d = result.to_dict()
    for key in ("pipeline", "previous_status", "current_status",
                "previous_failure_rate", "current_failure_rate",
                "recovered", "still_failing", "note"):
        assert key in d


def test_recheck_all_covers_all_pipelines():
    before = [
        make_metric("a", total=100, failed=50, status="error"),
        make_metric("b", total=100, failed=0, status="ok"),
    ]
    after = [
        make_metric("a", total=100, failed=0, status="ok"),
        make_metric("b", total=100, failed=0, status="ok"),
    ]
    results = recheck_all(before, after)
    assert len(results) == 2
    pipelines = {r.pipeline for r in results}
    assert pipelines == {"a", "b"}


def test_recheck_all_empty():
    assert recheck_all([], []) == []
