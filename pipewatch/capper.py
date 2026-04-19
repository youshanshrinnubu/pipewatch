"""Cap (clamp) metric values to configured bounds."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, List
from pipewatch.metrics import PipelineMetric


@dataclass
class CapConfig:
    max_failure_rate: float = 1.0
    max_total_records: Optional[int] = None
    max_failed_records: Optional[int] = None


@dataclass
class CapResult:
    original: PipelineMetric
    capped: PipelineMetric
    changed: bool

    def to_dict(self) -> dict:
        return {
            "pipeline": self.original.pipeline_name,
            "changed": self.changed,
            "original_failure_rate": (
                self.original.failed_records / self.original.total_records
                if self.original.total_records else 0.0
            ),
            "capped_failure_rate": (
                self.capped.failed_records / self.capped.total_records
                if self.capped.total_records else 0.0
            ),
        }


def _clamp(value: int, maximum: Optional[int]) -> int:
    if maximum is None:
        return value
    return min(value, maximum)


def cap_metric(metric: PipelineMetric, config: CapConfig) -> CapResult:
    total = _clamp(metric.total_records, config.max_total_records)
    failed = _clamp(metric.failed_records, config.max_failed_records)

    # Ensure failed does not exceed total after clamping
    if total > 0:
        max_allowed_failed = int(total * config.max_failure_rate)
        failed = min(failed, max_allowed_failed)
    else:
        failed = 0

    changed = (total != metric.total_records or failed != metric.failed_records)

    capped = PipelineMetric(
        pipeline_name=metric.pipeline_name,
        status=metric.status,
        total_records=total,
        failed_records=failed,
        duration_seconds=metric.duration_seconds,
        timestamp=metric.timestamp,
        extra=metric.extra,
    )
    return CapResult(original=metric, capped=capped, changed=changed)


def cap_all(metrics: List[PipelineMetric], config: CapConfig) -> List[CapResult]:
    return [cap_metric(m, config) for m in metrics]


def format_capped(results: List[CapResult]) -> str:
    if not results:
        return "No metrics to cap."
    lines = []
    for r in results:
        tag = "[changed]" if r.changed else "[unchanged]"
        fr = r.capped.failed_records / r.capped.total_records if r.capped.total_records else 0.0
        lines.append(f"{tag} {r.original.pipeline_name}: failure_rate={fr:.2%}")
    return "\n".join(lines)
