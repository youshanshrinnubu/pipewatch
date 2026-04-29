"""clamper.py — Clamp pipeline metric values to configurable min/max bounds.

Differs from capper.py in that it operates on the raw integer fields
(total_records, failed_records, duration_seconds) and enforces a hard
floor as well as a ceiling, returning a structured result with change flags.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class ClampConfig:
    min_total_records: int = 0
    max_total_records: int = 10_000_000
    min_failed_records: int = 0
    max_failed_records: int = 10_000_000
    min_duration_seconds: Optional[float] = None
    max_duration_seconds: Optional[float] = None


@dataclass
class ClampResult:
    original: PipelineMetric
    clamped: PipelineMetric
    fields_changed: list[str] = field(default_factory=list)

    @property
    def changed(self) -> bool:
        return len(self.fields_changed) > 0

    def to_dict(self) -> dict:
        return {
            "pipeline": self.original.pipeline_name,
            "changed": self.changed,
            "fields_changed": self.fields_changed,
            "original": {
                "total_records": self.original.total_records,
                "failed_records": self.original.failed_records,
                "duration_seconds": self.original.duration_seconds,
            },
            "clamped": {
                "total_records": self.clamped.total_records,
                "failed_records": self.clamped.failed_records,
                "duration_seconds": self.clamped.duration_seconds,
            },
        }


def _clamp(value: float, lo: Optional[float], hi: Optional[float]) -> float:
    if lo is not None and value < lo:
        return lo
    if hi is not None and value > hi:
        return hi
    return value


def clamp_metric(metric: PipelineMetric, cfg: ClampConfig) -> ClampResult:
    changes: list[str] = []

    new_total = int(_clamp(metric.total_records, cfg.min_total_records, cfg.max_total_records))
    if new_total != metric.total_records:
        changes.append("total_records")

    new_failed = int(_clamp(metric.failed_records, cfg.min_failed_records, cfg.max_failed_records))
    if new_failed != metric.failed_records:
        changes.append("failed_records")

    new_dur = metric.duration_seconds
    if new_dur is not None:
        clamped_dur = _clamp(new_dur, cfg.min_duration_seconds, cfg.max_duration_seconds)
        if clamped_dur != new_dur:
            changes.append("duration_seconds")
            new_dur = clamped_dur

    clamped = PipelineMetric(
        pipeline_name=metric.pipeline_name,
        status=metric.status,
        total_records=new_total,
        failed_records=new_failed,
        duration_seconds=new_dur,
        timestamp=metric.timestamp,
        extra=metric.extra,
    )
    return ClampResult(original=metric, clamped=clamped, fields_changed=changes)


def clamp_all(metrics: list[PipelineMetric], cfg: ClampConfig) -> list[ClampResult]:
    return [clamp_metric(m, cfg) for m in metrics]
