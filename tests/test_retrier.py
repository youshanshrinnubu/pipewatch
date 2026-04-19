import time
import pytest
from pipewatch.retrier import Retrier, RetryConfig, RetryRecord, RetryResult
from pipewatch.metrics import PipelineMetric


def make_metric(pipeline="test", total=100, failed=0, status="ok"):
    return PipelineMetric(pipeline, total, failed, status, time.time())


def test_should_retry_false_when_healthy():
    retrier = Retrier()
    m = make_metric(failed=10, total=100)
    assert not retrier.should_retry(m)


def test_should_retry_true_on_high_failure_rate():
    retrier = Retrier()
    m = make_metric(failed=60, total=100)
    assert retrier.should_retry(m)


def test_should_retry_true_on_error_status():
    retrier = Retrier()
    m = make_metric(failed=0, total=100, status="error")
    assert retrier.should_retry(m)


def test_should_retry_false_when_zero_records():
    retrier = Retrier()
    m = make_metric(failed=0, total=0)
    assert not retrier.should_retry(m)


def test_record_attempt_stores_record():
    retrier = Retrier()
    rec = retrier.record_attempt("pipe", 1, False, "timeout")
    assert rec.pipeline == "pipe"
    assert rec.attempt == 1
    assert not rec.succeeded
    assert rec.reason == "timeout"


def test_evaluate_returns_result_with_history():
    retrier = Retrier()
    m = make_metric(pipeline="pipe", failed=70, total=100)
    retrier.record_attempt("pipe", 1, False, "fail")
    retrier.record_attempt("pipe", 2, True, "ok")
    result = retrier.evaluate(m)
    assert result.pipeline == "pipe"
    assert result.total_attempts == 2
    assert result.succeeded
    assert len(result.records) == 2


def test_evaluate_no_history_returns_zero_attempts():
    retrier = Retrier()
    m = make_metric(pipeline="unknown")
    result = retrier.evaluate(m)
    assert result.total_attempts == 0
    assert not result.succeeded


def test_exhausted_true_after_max_retries():
    config = RetryConfig(max_retries=2)
    retrier = Retrier(config)
    retrier.record_attempt("p", 1, False, "x")
    retrier.record_attempt("p", 2, False, "x")
    assert retrier.exhausted("p")


def test_exhausted_false_before_max_retries():
    config = RetryConfig(max_retries=3)
    retrier = Retrier(config)
    retrier.record_attempt("p", 1, False, "x")
    assert not retrier.exhausted("p")


def test_to_dict_contains_expected_keys():
    retrier = Retrier()
    m = make_metric(pipeline="sales", failed=80, total=100)
    retrier.record_attempt("sales", 1, True, "retry")
    result = retrier.evaluate(m)
    d = result.to_dict()
    assert "pipeline" in d
    assert "total_attempts" in d
    assert "succeeded" in d
    assert "records" in d
    assert d["records"][0]["attempt"] == 1
