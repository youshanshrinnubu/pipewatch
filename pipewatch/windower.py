"""windower.py — Sliding window aggregation over pipeline metrics."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class WindowResult:
    pipeline: str
    window_seconds: int
    metric_count: int
    avg_failure_rate: float
    max_failure_rate: float
    min_failure_rate: float
    dominant_status: str
    earliest: Optional[datetime]
    latest: Optional[datetime]

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "window_seconds": self.window_seconds,
            "metric_count": self.metric_count,
            "avg_failure_rate": round(self.avg_failure_rate, 4),
            "max_failure_rate": round(self.max_failure_rate, 4),
            "min_failure_rate": round(self.min_failure_rate, 4),
            "dominant_status": self.dominant_status,
            "earliest": self.earliest.isoformat() if self.earliest else None,
            "latest": self.latest.isoformat() if self.latest else None,
        }


def _failure_rate(m: PipelineMetric) -> float:
    if m.total_records == 0:
        return 0.0
    return m.failed_records / m.total_records


def _dominant_status(metrics: List[PipelineMetric]) -> str:
    counts: Dict[str, int] = {}
    for m in metrics:
        counts[m.status] = counts.get(m.status, 0) + 1
    return max(counts, key=lambda s: counts[s])


def window_metrics(
    metrics: List[PipelineMetric],
    pipeline: str,
    window_seconds: int,
    reference_time: Optional[datetime] = None,
) -> Optional[WindowResult]:
    """Aggregate metrics for *pipeline* that fall within *window_seconds* of *reference_time*."""
    ref = reference_time or datetime.utcnow()
    cutoff = ref - timedelta(seconds=window_seconds)
    candidates = [
        m for m in metrics
        if m.pipeline == pipeline and m.timestamp is not None and m.timestamp >= cutoff
    ]
    if not candidates:
        return None
    rates = [_failure_rate(m) for m in candidates]
    timestamps = [m.timestamp for m in candidates if m.timestamp is not None]
    return WindowResult(
        pipeline=pipeline,
        window_seconds=window_seconds,
        metric_count=len(candidates),
        avg_failure_rate=sum(rates) / len(rates),
        max_failure_rate=max(rates),
        min_failure_rate=min(rates),
        dominant_status=_dominant_status(candidates),
        earliest=min(timestamps) if timestamps else None,
        latest=max(timestamps) if timestamps else None,
    )


def window_all(
    metrics: List[PipelineMetric],
    window_seconds: int,
    reference_time: Optional[datetime] = None,
) -> List[WindowResult]:
    """Return a WindowResult for every distinct pipeline present in *metrics*."""
    pipelines = list(dict.fromkeys(m.pipeline for m in metrics))
    results = []
    for p in pipelines:
        r = window_metrics(metrics, p, window_seconds, reference_time)
        if r is not None:
            results.append(r)
    return results
