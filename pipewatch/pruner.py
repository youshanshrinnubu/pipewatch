"""Pruner: remove old or stale metrics from a SnapshotStore."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional

from pipewatch.metrics import PipelineMetric
from pipewatch.snapshot import SnapshotStore


@dataclass
class PruneResult:
    pipeline: str
    removed: int
    retained: int

    def to_dict(self) -> dict:
        return {"pipeline": self.pipeline, "removed": self.removed, "retained": self.retained}


def prune_by_age(
    metrics: List[PipelineMetric], max_age_seconds: float
) -> List[PipelineMetric]:
    """Return only metrics newer than max_age_seconds."""
    cutoff = datetime.utcnow() - timedelta(seconds=max_age_seconds)
    return [m for m in metrics if m.timestamp >= cutoff]


def prune_by_count(
    metrics: List[PipelineMetric], max_count: int
) -> List[PipelineMetric]:
    """Keep only the most recent max_count metrics."""
    sorted_metrics = sorted(metrics, key=lambda m: m.timestamp)
    return sorted_metrics[-max_count:] if len(sorted_metrics) > max_count else sorted_metrics


def prune_store(
    store: SnapshotStore,
    max_age_seconds: Optional[float] = None,
    max_count: Optional[int] = None,
) -> List[PruneResult]:
    """Prune all pipelines in the store; returns per-pipeline results."""
    all_metrics = store.load_all()
    pipelines: dict = {}
    for m in all_metrics:
        pipelines.setdefault(m.pipeline, []).append(m)

    results = []
    pruned_all: List[PipelineMetric] = []
    for pipeline, metrics in pipelines.items():
        original = len(metrics)
        if max_age_seconds is not None:
            metrics = prune_by_age(metrics, max_age_seconds)
        if max_count is not None:
            metrics = prune_by_count(metrics, max_count)
        results.append(PruneResult(pipeline=pipeline, removed=original - len(metrics), retained=len(metrics)))
        pruned_all.extend(metrics)

    store.replace_all(pruned_all)
    return results
