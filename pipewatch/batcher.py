"""Batch metrics into fixed-size chunks for bulk processing."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class Batch:
    index: int
    metrics: List[PipelineMetric]

    def size(self) -> int:
        return len(self.metrics)

    def pipeline_names(self) -> List[str]:
        return [m.pipeline_name for m in self.metrics]

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "size": self.size(),
            "pipelines": self.pipeline_names(),
        }


@dataclass
class BatchResult:
    total_metrics: int
    batch_size: int
    batches: List[Batch] = field(default_factory=list)

    def count(self) -> int:
        return len(self.batches)

    def to_dict(self) -> dict:
        return {
            "total_metrics": self.total_metrics,
            "batch_size": self.batch_size,
            "batch_count": self.count(),
            "batches": [b.to_dict() for b in self.batches],
        }


def batch_metrics(
    metrics: List[PipelineMetric],
    batch_size: int = 10,
    pipeline: Optional[str] = None,
) -> BatchResult:
    """Split *metrics* into batches of *batch_size*.

    If *pipeline* is given, only metrics for that pipeline are included.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")

    items = (
        [m for m in metrics if m.pipeline_name == pipeline]
        if pipeline
        else list(metrics)
    )

    batches: List[Batch] = []
    for i in range(0, len(items), batch_size):
        chunk = items[i : i + batch_size]
        batches.append(Batch(index=len(batches), metrics=chunk))

    return BatchResult(
        total_metrics=len(items),
        batch_size=batch_size,
        batches=batches,
    )


def format_batched(result: BatchResult) -> str:
    lines = [
        f"Batches : {result.count()} (size={result.batch_size})",
        f"Total   : {result.total_metrics} metrics",
    ]
    for b in result.batches:
        names = ", ".join(b.pipeline_names()) or "(empty)"
        lines.append(f"  [{b.index}] {b.size()} metrics — {names}")
    return "\n".join(lines)
