import pytest
import time
from pipewatch.metrics import PipelineMetric
from pipewatch.sampler import sample_pipeline, sample_all, format_sampled, SampleResult


def make_metric(pipeline="sales", status="ok", failed=0, offset=0):
    return PipelineMetric(
        pipeline=pipeline,
        timestamp=time.time() - offset,
        status=status,
        total_records=100,
        failed_records=failed,
    )


def test_sample_pipeline_returns_none_for_empty():
    assert sample_pipeline([], "sales") is None


def test_sample_pipeline_returns_none_for_unknown():
    metrics = [make_metric("sales")]
    assert sample_pipeline(metrics, "other") is None


def test_sample_pipeline_returns_result():
    metrics = [make_metric(offset=i) for i in range(10)]
    result = sample_pipeline(metrics, "sales", n=4, seed=42)
    assert isinstance(result, SampleResult)
    assert result.pipeline == "sales"
    assert result.sample_size == 4
    assert result.total == 10
    assert len(result.sampled) == 4


def test_sample_pipeline_caps_at_available():
    metrics = [make_metric(offset=i) for i in range(3)]
    result = sample_pipeline(metrics, "sales", n=10, seed=0)
    assert result.sample_size == 3


def test_sample_pipeline_rate():
    metrics = [make_metric(offset=i) for i in range(10)]
    result = sample_pipeline(metrics, "sales", n=5, seed=1)
    assert result.rate == pytest.approx(0.5)


def test_sample_all_returns_per_pipeline():
    metrics = [
        make_metric("sales", offset=i) for i in range(5)
    ] + [
        make_metric("inventory", offset=i) for i in range(4)
    ]
    results = sample_all(metrics, n=3, seed=7)
    names = [r.pipeline for r in results]
    assert "sales" in names
    assert "inventory" in names


def test_sample_all_empty():
    assert sample_all([]) == []


def test_to_dict_keys():
    metrics = [make_metric(offset=i) for i in range(6)]
    result = sample_pipeline(metrics, "sales", n=3, seed=0)
    d = result.to_dict()
    assert "pipeline" in d
    assert "total" in d
    assert "sample_size" in d
    assert "rate" in d
    assert "sampled" in d


def test_format_sampled_no_results():
    assert format_sampled([]) == "No samples."


def test_format_sampled_contains_pipeline():
    metrics = [make_metric(offset=i) for i in range(5)]
    results = sample_all(metrics, n=2, seed=3)
    out = format_sampled(results)
    assert "sales" in out
