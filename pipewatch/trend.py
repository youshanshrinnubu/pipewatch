"""Trend analysis for pipeline metrics over time."""
from dataclasses import dataclass
from typing import List, Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class TrendResult:
    pipeline: str
    direction: str  # "improving", "degrading", "stable"
    avg_failure_rate: float
    min_failure_rate: float
    max_failure_rate: float
    sample_count: int

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "direction": self.direction,
            "avg_failure_rate": round(self.avg_failure_rate, 4),
            "min_failure_rate": round(self.min_failure_rate, 4),
            "max_failure_rate": round(self.max_failure_rate, 4),
            "sample_count": self.sample_count,
        }


def analyze_trend(metrics: List[PipelineMetric], threshold: float = 0.05) -> Optional[TrendResult]:
    """Analyze trend direction from an ordered list of metrics for a single pipeline."""
    if not metrics:
        return None

    rates = [m.failed_records / m.total_records if m.total_records > 0 else 0.0 for m in metrics]
    avg = sum(rates) / len(rates)
    direction = "stable"

    if len(rates) >= 2:
        first_half = rates[: len(rates) // 2]
        second_half = rates[len(rates) // 2 :]
        first_avg = sum(first_half) / len(first_half)
        second_avg = sum(second_half) / len(second_half)
        delta = second_avg - first_avg
        if delta > threshold:
            direction = "degrading"
        elif delta < -threshold:
            direction = "improving"

    return TrendResult(
        pipeline=metrics[0].pipeline_name,
        direction=direction,
        avg_failure_rate=avg,
        min_failure_rate=min(rates),
        max_failure_rate=max(rates),
        sample_count=len(metrics),
    )


def analyze_all_trends(metrics: List[PipelineMetric], threshold: float = 0.05) -> List[TrendResult]:
    """Group metrics by pipeline and analyze each."""
    grouped: dict = {}
    for m in metrics:
        grouped.setdefault(m.pipeline_name, []).append(m)
    return [analyze_trend(ms, threshold) for ms in grouped.values() if ms]
