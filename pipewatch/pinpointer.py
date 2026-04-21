"""Pinpointer: identify the single worst metric per pipeline based on a composite score."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class PinpointResult:
    pipeline: str
    worst_metric: PipelineMetric
    reason: str
    score: float  # 0.0 (worst) – 1.0 (best)


def to_dict(r: PinpointResult) -> dict:
    return {
        "pipeline": r.pipeline,
        "reason": r.reason,
        "score": round(r.score, 4),
        "metric": {
            "status": r.worst_metric.status,
            "total_records": r.worst_metric.total_records,
            "failed_records": r.worst_metric.failed_records,
            "duration_seconds": r.worst_metric.duration_seconds,
        },
    }


def _score(metric: PipelineMetric) -> float:
    """Lower score = worse health."""
    if metric.status == "error":
        return 0.0
    failure_rate = (
        metric.failed_records / metric.total_records
        if metric.total_records > 0
        else 0.0
    )
    base = 1.0 - failure_rate
    if metric.status == "warning":
        base *= 0.75
    return max(0.0, min(1.0, base))


def _reason(metric: PipelineMetric, score: float) -> str:
    if metric.status == "error":
        return "pipeline status is error"
    failure_rate = (
        metric.failed_records / metric.total_records
        if metric.total_records > 0
        else 0.0
    )
    if failure_rate >= 0.5:
        return f"failure rate {failure_rate:.1%} is critically high"
    if metric.status == "warning":
        return "pipeline status is warning"
    if failure_rate > 0:
        return f"failure rate {failure_rate:.1%} is elevated"
    return "no issues detected"


def pinpoint(pipeline: str, metrics: List[PipelineMetric]) -> Optional[PinpointResult]:
    """Return the single worst metric for *pipeline*."""
    candidates = [m for m in metrics if m.pipeline == pipeline]
    if not candidates:
        return None
    worst = min(candidates, key=_score)
    s = _score(worst)
    return PinpointResult(
        pipeline=pipeline,
        worst_metric=worst,
        reason=_reason(worst, s),
        score=s,
    )


def pinpoint_all(
    metrics: List[PipelineMetric],
) -> Dict[str, PinpointResult]:
    """Return the worst metric for every pipeline present in *metrics*."""
    pipelines = {m.pipeline for m in metrics}
    results = {}
    for p in sorted(pipelines):
        r = pinpoint(p, metrics)
        if r is not None:
            results[p] = r
    return results
