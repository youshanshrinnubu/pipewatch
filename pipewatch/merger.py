"""Merge metrics from multiple snapshot sources into a unified view."""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class MergedMetric:
    pipeline: str
    metrics: List[PipelineMetric]
    source_count: int

    def latest(self) -> Optional[PipelineMetric]:
        if not self.metrics:
            return None
        return max(self.metrics, key=lambda m: m.timestamp)

    def to_dict(self) -> dict:
        latest = self.latest()
        return {
            "pipeline": self.pipeline,
            "source_count": self.source_count,
            "metric_count": len(self.metrics),
            "latest": latest.to_dict() if latest else None,
        }


def merge_sources(sources: List[List[PipelineMetric]]) -> Dict[str, MergedMetric]:
    """Merge multiple lists of metrics by pipeline name."""
    grouped: Dict[str, List[PipelineMetric]] = {}
    pipeline_sources: Dict[str, set] = {}

    for idx, source in enumerate(sources):
        for metric in source:
            grouped.setdefault(metric.pipeline, []).append(metric)
            pipeline_sources.setdefault(metric.pipeline, set()).add(idx)

    return {
        name: MergedMetric(
            pipeline=name,
            metrics=metrics,
            source_count=len(pipeline_sources[name]),
        )
        for name, metrics in grouped.items()
    }


def format_merged(merged: Dict[str, MergedMetric]) -> str:
    if not merged:
        return "No merged metrics."
    lines = []
    for name, m in sorted(merged.items()):
        latest = m.latest()
        fr = f"{latest.failure_rate:.2%}" if latest else "n/a"
        status = latest.status if latest else "unknown"
        lines.append(
            f"  {name}: {m.metric_count} metric(s) from {m.source_count} source(s) "
            f"| status={status} failure_rate={fr}"
        )
    return "Merged Pipelines:\n" + "\n".join(lines)
