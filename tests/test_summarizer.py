"""Tests for pipewatch.summarizer."""
from __future__ import annotations
import json
from datetime import datetime, timezone
from unittest.mock import patch
from pipewatch.metrics import PipelineMetric
from pipewatch.summarizer import (
    summarize_metrics,
    format_summarizer_result,
    _note,
    _tier,
)


def make_metric(
    pipeline="pipe",
    status="ok",
    total=100,
    failed=0,
    ts: float = 1_000_000.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        status=status,
        total_records=total,
        failed_records=failed,
        timestamp=ts,
    )


# --- _note ---

def test_note_error_status():
    assert _note(0.0, "error") == "pipeline in error state"


def test_note_critical_failure_rate():
    assert _note(0.6, "ok") == "critical failure rate"


def test_note_elevated_failure_rate():
    assert _note(0.25, "ok") == "elevated failure rate"


def test_note_minor_failures():
    assert _note(0.05, "ok") == "minor failures detected"


def test_note_all_ok():
    assert _note(0.0, "ok") == "all records processed successfully"


# --- _tier ---

def test_tier_error_is_critical():
    assert _tier(0.0, "error") == "critical"


def test_tier_high_failure_rate_critical():
    assert _tier(0.5, "ok") == "critical"


def test_tier_warning_status():
    assert _tier(0.1, "warning") == "warning"


def test_tier_moderate_failure_rate_warning():
    assert _tier(0.2, "ok") == "warning"


def test_tier_healthy():
    assert _tier(0.0, "ok") == "healthy"


# --- summarize_metrics ---

def test_summarize_empty():
    result = summarize_metrics([])
    assert result.total_pipelines == 0
    assert result.lines == []


def test_summarize_single_healthy():
    m = make_metric(pipeline="alpha", total=200, failed=0)
    result = summarize_metrics([m])
    assert result.total_pipelines == 1
    assert result.healthy_count == 1
    assert result.warning_count == 0
    assert result.critical_count == 0
    assert result.lines[0].pipeline == "alpha"
    assert result.lines[0].failure_rate == 0.0


def test_summarize_picks_latest_per_pipeline():
    m1 = make_metric(pipeline="p", failed=0, ts=1000.0)
    m2 = make_metric(pipeline="p", failed=50, ts=2000.0)
    result = summarize_metrics([m1, m2])
    assert result.total_pipelines == 1
    assert result.lines[0].failed_records == 50


def test_summarize_multiple_pipelines_counts():
    metrics = [
        make_metric("a", total=100, failed=0),
        make_metric("b", total=100, failed=25, status="warning"),
        make_metric("c", total=100, failed=60, status="error"),
    ]
    result = summarize_metrics(metrics)
    assert result.total_pipelines == 3
    assert result.healthy_count == 1
    assert result.warning_count == 1
    assert result.critical_count == 1


def test_summarize_to_dict_structure():
    m = make_metric(pipeline="x", total=50, failed=5)
    result = summarize_metrics([m])
    d = result.to_dict()
    assert "total_pipelines" in d
    assert "lines" in d
    assert d["lines"][0]["pipeline"] == "x"


# --- format_summarizer_result ---

def test_format_contains_pipeline_name():
    m = make_metric(pipeline="mypipe", total=100, failed=10)
    result = summarize_metrics([m])
    text = format_summarizer_result(result)
    assert "mypipe" in text


def test_format_contains_counts_header():
    result = summarize_metrics([])
    text = format_summarizer_result(result)
    assert "Pipelines:" in text
    assert "Healthy:" in text


# --- CLI smoke test ---

def test_summarizer_cli_text(tmp_path):
    import json as _json
    from pipewatch.summarizer_cli import main

    snap = tmp_path / "snap.json"
    m = make_metric(pipeline="demo", total=100, failed=5)
    snap.write_text(_json.dumps([{
        "pipeline": m.pipeline,
        "status": m.status,
        "total_records": m.total_records,
        "failed_records": m.failed_records,
        "timestamp": m.timestamp,
    }]))
    rc = main(["--store", str(snap), "--format", "text"])
    assert rc == 0


def test_summarizer_cli_json(tmp_path, capsys):
    import json as _json
    from pipewatch.summarizer_cli import main

    snap = tmp_path / "snap.json"
    m = make_metric(pipeline="demo", total=100, failed=5)
    snap.write_text(_json.dumps([{
        "pipeline": m.pipeline,
        "status": m.status,
        "total_records": m.total_records,
        "failed_records": m.failed_records,
        "timestamp": m.timestamp,
    }]))
    main(["--store", str(snap), "--format", "json"])
    out = capsys.readouterr().out
    data = _json.loads(out)
    assert data["total_pipelines"] == 1
    assert data["lines"][0]["pipeline"] == "demo"
