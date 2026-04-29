"""Tests for pipewatch.zipper."""
from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.zipper import ZippedPair, format_zipped, zip_metrics


def make_metric(name: str, status: str = "ok", total: int = 100, failed: int = 0) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        status=status,
        total_records=total,
        failed_records=failed,
    )


# --- zip_metrics ---

def test_zip_empty_both():
    result = zip_metrics([], [])
    assert result == []


def test_zip_single_matching_pair():
    left = [make_metric("sales")]
    right = [make_metric("sales", status="warning")]
    pairs = zip_metrics(left, right)
    assert len(pairs) == 1
    assert pairs[0].pipeline == "sales"
    assert pairs[0].left is not None
    assert pairs[0].right is not None
    assert pairs[0].both_present()


def test_zip_only_left():
    left = [make_metric("sales")]
    right = []
    pairs = zip_metrics(left, right)
    assert len(pairs) == 1
    assert pairs[0].only_left()
    assert not pairs[0].only_right()
    assert not pairs[0].both_present()


def test_zip_only_right():
    left = []
    right = [make_metric("inventory")]
    pairs = zip_metrics(left, right)
    assert len(pairs) == 1
    assert pairs[0].only_right()
    assert not pairs[0].only_left()


def test_zip_multiple_pipelines_sorted():
    left = [make_metric("z_pipe"), make_metric("a_pipe")]
    right = [make_metric("m_pipe"), make_metric("a_pipe")]
    pairs = zip_metrics(left, right)
    names = [p.pipeline for p in pairs]
    assert names == sorted(names)


def test_zip_disjoint_sets():
    left = [make_metric("alpha")]
    right = [make_metric("beta")]
    pairs = zip_metrics(left, right)
    assert len(pairs) == 2
    alpha = next(p for p in pairs if p.pipeline == "alpha")
    beta = next(p for p in pairs if p.pipeline == "beta")
    assert alpha.only_left()
    assert beta.only_right()


# --- ZippedPair.to_dict ---

def test_to_dict_both_present():
    pair = ZippedPair(
        pipeline="sales",
        left=make_metric("sales", status="ok"),
        right=make_metric("sales", status="warning"),
    )
    d = pair.to_dict()
    assert d["pipeline"] == "sales"
    assert d["both_present"] is True
    assert d["left"]["status"] == "ok"
    assert d["right"]["status"] == "warning"


def test_to_dict_only_left():
    pair = ZippedPair(pipeline="x", left=make_metric("x"), right=None)
    d = pair.to_dict()
    assert d["only_left"] is True
    assert d["right"] is None


# --- format_zipped ---

def test_format_zipped_empty():
    assert "No pipelines" in format_zipped([])


def test_format_zipped_contains_pipeline_name():
    pairs = zip_metrics([make_metric("sales")], [make_metric("sales", status="error")])
    output = format_zipped(pairs)
    assert "sales" in output
    assert "status changed" in output


def test_format_zipped_only_left_note():
    pairs = zip_metrics([make_metric("orphan")], [])
    output = format_zipped(pairs)
    assert "only in left" in output


def test_format_zipped_only_right_note():
    pairs = zip_metrics([], [make_metric("newcomer")])
    output = format_zipped(pairs)
    assert "only in right" in output
