"""Flatten nested pipeline metrics into a single-level dict representation."""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class FlatMetric:
    pipeline: str
    status: str
    total_records: int
    failed_records: int
    failure_rate: float
    duration_seconds: Optional[float]
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "pipeline": self.pipeline,
            "status": self.status,
            "total_records": self.total_records,
            "failed_records": self.failed_records,
            "failure_rate": round(self.failure_rate, 4),
            "duration_seconds": self.duration_seconds,
        }
        d.update(self.extra)
        return d


def _failure_rate(metric: PipelineMetric) -> float:
    if metric.total_records == 0:
        return 0.0
    return metric.failed_records / metric.total_records


def flatten(metric: PipelineMetric, extra: Optional[Dict[str, Any]] = None) -> FlatMetric:
    """Convert a PipelineMetric into a FlatMetric."""
    return FlatMetric(
        pipeline=metric.pipeline_name,
        status=metric.status,
        total_records=metric.total_records,
        failed_records=metric.failed_records,
        failure_rate=_failure_rate(metric),
        duration_seconds=getattr(metric, "duration_seconds", None),
        extra=extra or {},
    )


def flatten_all(
    metrics: List[PipelineMetric],
    extra_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[FlatMetric]:
    """Flatten a list of PipelineMetrics."""
    extra_map = extra_map or {}
    return [flatten(m, extra_map.get(m.pipeline_name)) for m in metrics]


def format_flat(flat_metrics: List[FlatMetric]) -> str:
    if not flat_metrics:
        return "No metrics to display."
    lines = []
    for fm in flat_metrics:
        rate_pct = f"{fm.failure_rate * 100:.1f}%"
        dur = f"{fm.duration_seconds:.1f}s" if fm.duration_seconds is not None else "n/a"
        lines.append(
            f"[{fm.status.upper()}] {fm.pipeline} | "
            f"records={fm.total_records} failed={fm.failed_records} "
            f"rate={rate_pct} dur={dur}"
        )
    return "\n".join(lines)
