"""Tests for pipewatch.fencer."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.fencer import Fencer, FenceConfig, FenceState
from pipewatch.metrics import PipelineMetric


def make_metric(
    pipeline: str = "test",
    total: int = 1000,
    failed: int = 0,
    status: str = "ok",
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=pipeline,
        total_records=total,
        failed_records=failed,
        status=status,
        timestamp=datetime.now(timezone.utc).isoformat(),
        duration_seconds=1.0,
        extra={},
    )


def test_fence_starts_closed():
    fencer = Fencer()
    result = fencer.check(make_metric(failed=0))
    assert not result.state.open
    assert not result.tripped
    assert not result.reset


def test_fence_does_not_trip_below_threshold():
    cfg = FenceConfig(trip_threshold=0.3, trip_count=3)
    fencer = Fencer(cfg)
    for _ in range(5):
        r = fencer.check(make_metric(failed=200))  # 20% < 30%
    assert not r.state.open


def test_fence_trips_after_consecutive_bad_checks():
    cfg = FenceConfig(trip_threshold=0.3, trip_count=3)
    fencer = Fencer(cfg)
    results = [fencer.check(make_metric(failed=400)) for _ in range(3)]
    assert results[-1].tripped
    assert results[-1].state.open


def test_fence_does_not_trip_before_count_reached():
    cfg = FenceConfig(trip_threshold=0.3, trip_count=3)
    fencer = Fencer(cfg)
    r1 = fencer.check(make_metric(failed=400))
    r2 = fencer.check(make_metric(failed=400))
    assert not r1.tripped
    assert not r2.tripped
    assert not r2.state.open


def test_fence_resets_after_good_checks_while_open():
    cfg = FenceConfig(trip_threshold=0.3, trip_count=2, reset_count=2)
    fencer = Fencer(cfg)
    # trip it
    fencer.check(make_metric(failed=400))
    fencer.check(make_metric(failed=400))
    # now close it
    fencer.check(make_metric(failed=0))
    r = fencer.check(make_metric(failed=0))
    assert r.reset
    assert not r.state.open


def test_fence_resets_bad_counter_on_good_check():
    cfg = FenceConfig(trip_threshold=0.3, trip_count=3)
    fencer = Fencer(cfg)
    fencer.check(make_metric(failed=400))
    fencer.check(make_metric(failed=400))
    fencer.check(make_metric(failed=0))   # good — resets counter
    r = fencer.check(make_metric(failed=400))
    assert not r.state.open   # only 1 bad again


def test_error_status_counts_as_bad():
    cfg = FenceConfig(trip_threshold=0.5, trip_count=2)
    fencer = Fencer(cfg)
    fencer.check(make_metric(failed=0, status="error"))
    r = fencer.check(make_metric(failed=0, status="error"))
    assert r.tripped


def test_check_all_returns_one_result_per_metric():
    fencer = Fencer()
    metrics = [make_metric(pipeline=f"p{i}") for i in range(5)]
    results = fencer.check_all(metrics)
    assert len(results) == 5


def test_states_returns_all_seen_pipelines():
    fencer = Fencer()
    fencer.check(make_metric(pipeline="alpha"))
    fencer.check(make_metric(pipeline="beta"))
    names = {s.pipeline for s in fencer.states()}
    assert names == {"alpha", "beta"}


def test_to_dict_contains_expected_keys():
    fencer = Fencer()
    r = fencer.check(make_metric())
    d = r.to_dict()
    assert "state" in d
    assert "tripped" in d
    assert "reset" in d
    assert "open" in d["state"]
