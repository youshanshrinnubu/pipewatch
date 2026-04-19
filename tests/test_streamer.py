"""Tests for pipewatch.streamer."""
from __future__ import annotations

import time
from typing import List

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.alerts import AlertManager
from pipewatch.streamer import MetricStreamer, StreamConfig, StreamEvent


def make_metric(
    name: str = "pipe",
    status: str = "ok",
    total: int = 100,
    failed: int = 0,
) -> PipelineMetric:
    return PipelineMetric(name, status, total, failed, time.time())


def _am() -> AlertManager:
    return AlertManager()


def test_stream_yields_one_event_per_metric():
    metrics = [make_metric("a"), make_metric("b")]
    streamer = MetricStreamer(_am())
    events = list(streamer.stream(lambda: metrics))
    assert len(events) == 2


def test_stream_event_has_correct_pipeline():
    metrics = [make_metric("sales")]
    streamer = MetricStreamer(_am())
    events = list(streamer.stream(lambda: metrics))
    assert events[0].metric.pipeline_name == "sales"


def test_stream_sequence_increments():
    metrics = [make_metric("a"), make_metric("b"), make_metric("c")]
    streamer = MetricStreamer(_am())
    events = list(streamer.stream(lambda: metrics))
    seqs = [e.sequence for e in events]
    assert seqs == [1, 2, 3]


def test_stream_no_alerts_for_healthy_metric():
    metrics = [make_metric("pipe", "ok", 100, 0)]
    streamer = MetricStreamer(_am())
    events = list(streamer.stream(lambda: metrics))
    assert events[0].alerts == []


def test_stream_alerts_fired_on_high_failure_rate():
    metrics = [make_metric("pipe", "ok", 100, 60)]
    streamer = MetricStreamer(_am())
    events = list(streamer.stream(lambda: metrics))
    assert len(events[0].alerts) > 0


def test_stream_filter_by_pipeline():
    metrics = [make_metric("sales"), make_metric("inventory")]
    cfg = StreamConfig(pipelines=["sales"])
    streamer = MetricStreamer(_am(), cfg)
    events = list(streamer.stream(lambda: metrics))
    assert len(events) == 1
    assert events[0].metric.pipeline_name == "sales"


def test_stream_max_events_respected():
    metrics = [make_metric(f"p{i}") for i in range(10)]
    cfg = StreamConfig(max_events=3)
    streamer = MetricStreamer(_am(), cfg)
    events = list(streamer.stream(lambda: metrics))
    assert len(events) == 3


def test_to_dict_contains_required_keys():
    metric = make_metric("orders", "ok", 50, 5)
    am = _am()
    alerts = am.evaluate(metric)
    event = StreamEvent(metric=metric, alerts=alerts, sequence=7)
    d = event.to_dict()
    assert d["sequence"] == 7
    assert d["pipeline"] == "orders"
    assert "timestamp" in d
    assert "alerts" in d
