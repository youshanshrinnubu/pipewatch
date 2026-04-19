"""Normalize pipeline metric values to a standard scale."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class NormalizedMetric:
    pipeline: str
    status: str
    total_records: int
    failed_records: int
    failure_rate: float          # 0.0 – 1.0
    normalized_score: float      # 0.0 – 1.0  (1 = perfect)
    duration_seconds: Optional[float] = None
    extra: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "status": self.status,
            "total_records": self.total_records,
            "failed_records": self.failed_records,
            "failure_rate": round(self.failure_rate, 6),
            "normalized_score": round(self.normalized_score, 6),
            "duration_seconds": self.duration_seconds,
            "extra": self.extra,
        }


def _failure_rate(m: PipelineMetric) -> float:
    if m.total_records == 0:
        return 0.0
    return m.failed_records / m.total_records


def _normalized_score(failure_rate: float, status: str) -> float:
    """Score in [0, 1]: penalise failure rate and error status."""
    if status == "error":
        return 0.0
    base = 1.0 - failure_rate
    if status == "warning":
        base *= 0.8
    return max(0.0, min(1.0, base))


def normalize(metric: PipelineMetric) -> NormalizedMetric:
    fr = _failure_rate(metric)
    score = _normalized_score(fr, metric.status)
    return NormalizedMetric(
        pipeline=metric.pipeline,
        status=metric.status,
        total_records=metric.total_records,
        failed_records=metric.failed_records,
        failure_rate=fr,
        normalized_score=score,
        duration_seconds=getattr(metric, "duration_seconds", None),
        extra=getattr(metric, "extra", {}),
    )


def normalize_all(metrics: List[PipelineMetric]) -> List[NormalizedMetric]:
    return [normalize(m) for m in metrics]


def format_normalized(results: List[NormalizedMetric]) -> str:
    if not results:
        return "No metrics to normalize."
    lines = [f"{'Pipeline':<30} {'Status':<10} {'Failure Rate':>14} {'Score':>8}"]
    lines.append("-" * 66)
    for r in results:
        lines.append(
            f"{r.pipeline:<30} {r.status:<10} {r.failure_rate:>13.2%} {r.normalized_score:>8.4f}"
        )
    return "\n".join(lines)
