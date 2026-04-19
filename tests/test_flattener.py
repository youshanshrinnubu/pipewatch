import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.flattener import flatten, flatten_all, format_flat, FlatMetric


def make_metric(pipeline="pipe1", status="ok", total=100, failed=5, duration=None):
    m = PipelineMetric(pipeline_name=pipeline, status=status, total_records=total, failed_records=failed)
    if duration is not None:
        m.duration_seconds = duration
    return m


def test_flatten_basic_fields():
    m = make_metric()
    fm = flatten(m)
    assert fm.pipeline == "pipe1"
    assert fm.status == "ok"
    assert fm.total_records == 100
    assert fm.failed_records == 5


def test_flatten_failure_rate():
    m = make_metric(total=200, failed=10)
    fm = flatten(m)
    assert abs(fm.failure_rate - 0.05) < 1e-6


def test_flatten_zero_records():
    m = make_metric(total=0, failed=0)
    fm = flatten(m)
    assert fm.failure_rate == 0.0


def test_flatten_with_extra():
    m = make_metric()
    fm = flatten(m, extra={"team": "data-eng"})
    assert fm.extra["team"] == "data-eng"
    d = fm.to_dict()
    assert d["team"] == "data-eng"


def test_flatten_to_dict_keys():
    m = make_metric()
    d = flatten(m).to_dict()
    for key in ("pipeline", "status", "total_records", "failed_records", "failure_rate", "duration_seconds"):
        assert key in d


def test_flatten_all_returns_one_per_metric():
    metrics = [make_metric("a"), make_metric("b"), make_metric("c")]
    result = flatten_all(metrics)
    assert len(result) == 3
    assert [fm.pipeline for fm in result] == ["a", "b", "c"]


def test_flatten_all_with_extra_map():
    metrics = [make_metric("sales"), make_metric("inventory")]
    extra_map = {"sales": {"owner": "alice"}}
    result = flatten_all(metrics, extra_map=extra_map)
    assert result[0].extra.get("owner") == "alice"
    assert result[1].extra == {}


def test_format_flat_empty():
    assert format_flat([]) == "No metrics to display."


def test_format_flat_contains_pipeline_name():
    m = make_metric(pipeline="my_pipe", status="ok", total=50, failed=2)
    fm = flatten(m)
    out = format_flat([fm])
    assert "my_pipe" in out
    assert "OK" in out


def test_format_flat_shows_failure_rate():
    m = make_metric(total=100, failed=25)
    fm = flatten(m)
    out = format_flat([fm])
    assert "25.0%" in out
