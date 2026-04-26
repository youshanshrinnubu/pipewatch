"""trimmer.py — Remove low-signal metrics from a collection based on configurable thresholds."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class TrimResult:
    pipeline: str
    kept: List[PipelineMetric]
    removed: List[PipelineMetric]
    reason: str

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "kept": len(self.kept),
            "removed": len(self.removed),
            "reason": self.reason,
        }


@dataclass
class TrimConfig:
    min_total_records: int = 0
    max_failure_rate: float = 1.0
    require_status: Optional[List[str]] = field(default=None)


def _failure_rate(m: PipelineMetric) -> float:
    if m.total_records == 0:
        return 0.0
    return m.failed_records / m.total_records


def trim(metrics: List[PipelineMetric], config: TrimConfig) -> TrimResult:
    """Trim metrics that fall below quality thresholds."""
    if not metrics:
        pipeline = ""
        return TrimResult(pipeline=pipeline, kept=[], removed=[], reason="empty")

    pipeline = metrics[0].pipeline_name
    kept: List[PipelineMetric] = []
    removed: List[PipelineMetric] = []

    for m in metrics:
        reasons = []
        if m.total_records < config.min_total_records:
            reasons.append(f"total_records<{config.min_total_records}")
        if _failure_rate(m) > config.max_failure_rate:
            reasons.append(f"failure_rate>{config.max_failure_rate:.2f}")
        if config.require_status and m.status not in config.require_status:
            reasons.append(f"status not in {config.require_status}")

        if reasons:
            removed.append(m)
        else:
            kept.append(m)

    reason = "threshold" if removed else "none"
    return TrimResult(pipeline=pipeline, kept=kept, removed=removed, reason=reason)


def trim_all(
    metrics: List[PipelineMetric], config: TrimConfig
) -> List[TrimResult]:
    """Trim metrics grouped by pipeline."""
    pipelines: dict = {}
    for m in metrics:
        pipelines.setdefault(m.pipeline_name, []).append(m)
    return [trim(group, config) for group in pipelines.values()]


def format_trimmed(results: List[TrimResult]) -> str:
    if not results:
        return "No trim results."
    lines = []
    for r in results:
        lines.append(
            f"{r.pipeline}: kept={len(r.kept)} removed={len(r.removed)} reason={r.reason}"
        )
    return "\n".join(lines)
