import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.validator import (
    validate_metric,
    validate_all,
    format_validation,
    ValidationResult,
)


def make_metric(**kwargs) -> PipelineMetric:
    defaults = dict(
        pipeline_name="sales",
        total_records=100,
        failed_records=2,
        status="ok",
        duration_seconds=1.5,
    )
    defaults.update(kwargs)
    return PipelineMetric(**defaults)


def test_valid_metric_passes():
    result = validate_metric(make_metric())
    assert result.valid
    assert result.issues == []


def test_empty_pipeline_name_is_error():
    result = validate_metric(make_metric(pipeline_name=""))
    assert not result.valid
    fields = [i.field for i in result.issues]
    assert "pipeline_name" in fields


def test_negative_total_records_is_error():
    result = validate_metric(make_metric(total_records=-1))
    assert not result.valid
    assert any(i.field == "total_records" for i in result.issues)


def test_negative_failed_records_is_error():
    result = validate_metric(make_metric(failed_records=-5))
    assert not result.valid
    assert any(i.field == "failed_records" for i in result.issues)


def test_failed_exceeds_total_is_error():
    result = validate_metric(make_metric(total_records=10, failed_records=20))
    assert not result.valid
    assert any(i.field == "failed_records" for i in result.issues)


def test_unknown_status_is_warning_not_error():
    result = validate_metric(make_metric(status="banana"))
    issues = [i for i in result.issues if i.field == "status"]
    assert issues
    assert issues[0].severity == "warning"
    assert result.valid  # warnings don't fail validation


def test_negative_duration_is_error():
    result = validate_metric(make_metric(duration_seconds=-0.1))
    assert not result.valid
    assert any(i.field == "duration_seconds" for i in result.issues)


def test_none_duration_is_ok():
    result = validate_metric(make_metric(duration_seconds=None))
    assert result.valid


def test_validate_all_returns_one_per_metric():
    metrics = [make_metric(pipeline_name="a"), make_metric(pipeline_name="b")]
    results = validate_all(metrics)
    assert len(results) == 2


def test_format_validation_shows_pass():
    results = [validate_metric(make_metric())]
    text = format_validation(results)
    assert "PASS" in text
    assert "sales" in text


def test_format_validation_shows_fail_and_field():
    results = [validate_metric(make_metric(total_records=-1))]
    text = format_validation(results)
    assert "FAIL" in text
    assert "total_records" in text


def test_format_validation_empty():
    text = format_validation([])
    assert "No metrics" in text
