"""Watchdog: detect stale pipelines that haven't reported metrics recently."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class StalenessResult:
    pipeline: str
    last_seen: datetime
    age_seconds: float
    is_stale: bool
    severity: str  # "ok", "warning", "critical"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "last_seen": self.last_seen.isoformat(),
            "age_seconds": round(self.age_seconds, 2),
            "is_stale": self.is_stale,
            "severity": self.severity,
        }


def _now() -> datetime:
    return datetime.now(timezone.utc)


def check_staleness(
    metrics: List[PipelineMetric],
    warning_seconds: float = 300.0,
    critical_seconds: float = 900.0,
) -> List[StalenessResult]:
    """Return a staleness result for the most recent metric per pipeline."""
    latest: dict[str, PipelineMetric] = {}
    for m in metrics:
        if m.pipeline not in latest or m.timestamp > latest[m.pipeline].timestamp:
            latest[m.pipeline] = m

    now = _now()
    results = []
    for pipeline, m in sorted(latest.items()):
        ts = m.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age = (now - ts).total_seconds()
        if age >= critical_seconds:
            severity = "critical"
            stale = True
        elif age >= warning_seconds:
            severity = "warning"
            stale = True
        else:
            severity = "ok"
            stale = False
        results.append(StalenessResult(pipeline, ts, age, stale, severity))
    return results


def check_all_staleness(
    metrics_by_pipeline: dict[str, List[PipelineMetric]],
    warning_seconds: float = 300.0,
    critical_seconds: float = 900.0,
) -> List[StalenessResult]:
    all_metrics = [m for ms in metrics_by_pipeline.values() for m in ms]
    return check_staleness(all_metrics, warning_seconds, critical_seconds)
