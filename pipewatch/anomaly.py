from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.metrics import PipelineMetric


@dataclass
class AnomalyResult:
    pipeline: str
    metric: PipelineMetric
    reason: str
    severity: str  # "warning" | "critical"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "reason": self.reason,
            "severity": self.severity,
            "failure_rate": self.metric.failure_rate,
            "status": self.metric.status,
        }


def detect_anomalies(
    metrics: List[PipelineMetric],
    failure_rate_warning: float = 0.1,
    failure_rate_critical: float = 0.3,
    spike_multiplier: float = 3.0,
    history: Optional[List[PipelineMetric]] = None,
) -> List[AnomalyResult]:
    results: List[AnomalyResult] = []
    history_map: dict = {}
    if history:
        for m in history:
            history_map.setdefault(m.pipeline, []).append(m.failure_rate)

    for m in metrics:
        if m.status == "error":
            results.append(AnomalyResult(m.pipeline, m, "status is error", "critical"))
            continue

        fr = m.failure_rate
        if fr >= failure_rate_critical:
            results.append(AnomalyResult(m.pipeline, m, f"failure_rate {fr:.2%} exceeds critical threshold", "critical"))
        elif fr >= failure_rate_warning:
            results.append(AnomalyResult(m.pipeline, m, f"failure_rate {fr:.2%} exceeds warning threshold", "warning"))

        past = history_map.get(m.pipeline)
        if past and len(past) >= 2:
            avg = sum(past) / len(past)
            if avg > 0 and fr >= avg * spike_multiplier:
                results.append(AnomalyResult(m.pipeline, m, f"failure_rate spike: {fr:.2%} vs avg {avg:.2%}", "warning"))

    return results


def detect_all_anomalies(
    metrics_by_pipeline: dict,
    **kwargs,
) -> List[AnomalyResult]:
    all_metrics = [m for ms in metrics_by_pipeline.values() for m in ms]
    return detect_anomalies(all_metrics, **kwargs)
