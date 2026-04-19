import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.digester import (
    digest_pipeline,
    digest_all,
    format_digest,
    DigestEntry,
)


def make_metric(pipeline="pipe", status="ok", processed=100, failed=0):
    return PipelineMetric(
        pipeline=pipeline,
        status=status,
        records_processed=processed,
        records_failed=failed,
        duration_seconds=1.0,
    )


def test_digest_empty_metrics():
    entry = digest_pipeline("pipe", [])
    assert entry.total_metrics == 0
    assert entry.overall_status == "unknown"


def test_digest_all_healthy():
    metrics = [make_metric(failed=0) for _ in range(3)]
    entry = digest_pipeline("pipe", metrics)
    assert entry.healthy_count == 3
    assert entry.warning_count == 0
    assert entry.critical_count == 0
    assert entry.overall_status == "healthy"


def test_digest_warning_on_moderate_failure():
    metrics = [make_metric(processed=100, failed=25)]
    entry = digest_pipeline("pipe", metrics)
    assert entry.warning_count == 1
    assert entry.overall_status == "warning"


def test_digest_critical_on_high_failure():
    metrics = [make_metric(processed=100, failed=60)]
    entry = digest_pipeline("pipe", metrics)
    assert entry.critical_count == 1
    assert entry.overall_status == "critical"


def test_digest_critical_on_error_status():
    metrics = [make_metric(status="error", failed=0)]
    entry = digest_pipeline("pipe", metrics)
    assert entry.overall_status == "critical"


def test_digest_overall_critical_when_any_critical():
    metrics = [
        make_metric(failed=0),
        make_metric(failed=60),
    ]
    entry = digest_pipeline("pipe", metrics)
    assert entry.overall_status == "critical"


def test_digest_all_groups_by_pipeline():
    metrics = [
        make_metric(pipeline="a", failed=0),
        make_metric(pipeline="b", failed=60),
        make_metric(pipeline="a", failed=0),
    ]
    entries = digest_all(metrics)
    assert len(entries) == 2
    names = [e.pipeline for e in entries]
    assert "a" in names and "b" in names


def test_digest_all_empty():
    assert digest_all([]) == []


def test_format_digest_no_entries():
    result = format_digest([])
    assert "No pipelines" in result


def test_format_digest_contains_pipeline_name():
    metrics = [make_metric(pipeline="my_pipe", failed=0)]
    entries = digest_all(metrics)
    output = format_digest(entries)
    assert "my_pipe" in output
    assert "HEALTHY" in output


def test_to_dict_keys():
    entry = digest_pipeline("pipe", [make_metric()])
    d = entry.to_dict()
    assert "pipeline" in d
    assert "overall_status" in d
    assert "avg_failure_rate" in d
