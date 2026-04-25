"""Tests for pipewatch.stacker."""
from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.stacker import Stack, Stacker


def make_metric(name: str = "pipe", status: str = "ok",
                total: int = 100, failed: int = 0,
                ts: int = 1_000_000) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        status=status,
        total_records=total,
        failed_records=failed,
        timestamp=ts,
    )


# ---------------------------------------------------------------------------
# Stack unit tests
# ---------------------------------------------------------------------------

def test_stack_starts_empty():
    s = Stack(pipeline="p", capacity=3)
    assert s.size() == 0
    assert s.peek() is None
    assert s.all() == []


def test_stack_push_and_peek():
    s = Stack(pipeline="p", capacity=3)
    m = make_metric(ts=1)
    s.push(m)
    assert s.peek() is m
    assert s.size() == 1


def test_stack_all_oldest_first():
    s = Stack(pipeline="p", capacity=5)
    metrics = [make_metric(ts=i) for i in range(3)]
    for m in metrics:
        s.push(m)
    assert s.all() == metrics


def test_stack_evicts_oldest_at_capacity():
    s = Stack(pipeline="p", capacity=2)
    m1 = make_metric(ts=1)
    m2 = make_metric(ts=2)
    m3 = make_metric(ts=3)
    s.push(m1)
    s.push(m2)
    s.push(m3)  # m1 evicted
    assert s.size() == 2
    assert m1 not in s.all()
    assert s.all() == [m2, m3]


def test_stack_to_dict_keys():
    s = Stack(pipeline="sales", capacity=3)
    s.push(make_metric(name="sales"))
    d = s.to_dict()
    assert d["pipeline"] == "sales"
    assert d["capacity"] == 3
    assert d["size"] == 1
    assert isinstance(d["metrics"], list)


# ---------------------------------------------------------------------------
# Stacker unit tests
# ---------------------------------------------------------------------------

def test_stacker_invalid_capacity():
    with pytest.raises(ValueError):
        Stacker(capacity=0)


def test_stacker_push_creates_stack():
    st = Stacker(capacity=5)
    st.push(make_metric(name="alpha"))
    assert "alpha" in st.pipelines()


def test_stacker_push_all():
    st = Stacker(capacity=5)
    ms = [make_metric(name="a"), make_metric(name="b"), make_metric(name="a")]
    st.push_all(ms)
    assert st.get("a").size() == 2
    assert st.get("b").size() == 1


def test_stacker_get_returns_none_for_unknown():
    st = Stacker()
    assert st.get("nope") is None


def test_stacker_all_stacks_sorted():
    st = Stacker(capacity=3)
    for name in ["zeta", "alpha", "mu"]:
        st.push(make_metric(name=name))
    names = [s.pipeline for s in st.all_stacks()]
    assert names == sorted(names)


def test_stacker_format_text_contains_pipeline():
    st = Stacker(capacity=3)
    st.push(make_metric(name="inventory", status="warning"))
    text = st.format_text()
    assert "inventory" in text
    assert "warning" in text


def test_stacker_format_text_empty():
    st = Stacker()
    assert st.format_text() == "No stacks."
