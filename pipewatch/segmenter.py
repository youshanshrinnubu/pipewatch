"""Segment metrics into named buckets based on failure rate thresholds."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class Segment:
    name: str
    min_failure_rate: float
    max_failure_rate: float
    metrics: List[PipelineMetric] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "min_failure_rate": self.min_failure_rate,
            "max_failure_rate": self.max_failure_rate,
            "count": len(self.metrics),
            "pipelines": [m.pipeline_name for m in self.metrics],
        }


@dataclass
class SegmentResult:
    segments: Dict[str, Segment]

    def get(self, name: str) -> Optional[Segment]:
        return self.segments.get(name)

    def to_dict(self) -> dict:
        return {name: seg.to_dict() for name, seg in self.segments.items()}


DEFAULT_SEGMENTS = [
    ("healthy", 0.0, 0.05),
    ("degraded", 0.05, 0.20),
    ("critical", 0.20, 1.01),
]


def _failure_rate(metric: PipelineMetric) -> float:
    if metric.total_records == 0:
        return 0.0
    return metric.failed_records / metric.total_records


def segment(
    metrics: List[PipelineMetric],
    segments: Optional[List[tuple]] = None,
) -> SegmentResult:
    """Assign each metric to a named segment based on its failure rate."""
    if segments is None:
        segments = DEFAULT_SEGMENTS

    buckets: Dict[str, Segment] = {
        name: Segment(name=name, min_failure_rate=lo, max_failure_rate=hi)
        for name, lo, hi in segments
    }

    for metric in metrics:
        rate = _failure_rate(metric)
        for name, lo, hi in segments:
            if lo <= rate < hi:
                buckets[name].metrics.append(metric)
                break

    return SegmentResult(segments=buckets)


def format_segmented(result: SegmentResult) -> str:
    lines = ["Pipeline Segments:", "-" * 32]
    for name, seg in result.segments.items():
        lines.append(
            f"  {name:<12} [{seg.min_failure_rate:.0%} – {seg.max_failure_rate:.0%})"
            f"  count={len(seg.metrics)}"
        )
        for m in seg.metrics:
            lines.append(f"    - {m.pipeline_name}")
    return "\n".join(lines)
